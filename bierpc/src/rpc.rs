use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::serialize::DeserializeVerified;
use crate::serialize::SerializeVerified;
use std::io;
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::ParseIntError;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout, Sleep};
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig as TlsClientConfig, ServerConfig as TlsServerConfig};
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use bier_derive::{Deserialize, DeserializeVerified, Serialize, SerializeVerified};
use crate::error::{RpcError, RpcResult};
use crate::serialize::{Deserialize, Serialize};

pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1024 * 1024;
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialOrd, PartialEq, Debug, Serialize, Deserialize, SerializeVerified, DeserializeVerified)]
pub struct Target {
    pub port: u16,
    pub addr: Ipv4Addr
}

impl Target {
    pub fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(self.addr, self.port))
    }

    pub fn new(addr: Ipv4Addr, port: u16) -> Self {
        Self {
            addr,
            port
        }
    }
}

impl Into<SocketAddr> for Target {
    fn into(self) -> SocketAddr {
        self.to_socket_addr()
    }
}

impl Display for Target {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

#[derive(Debug)]
pub enum TargetParseError {
    PortParseErr(ParseIntError),
    ParseAddrErr,
    NoPortErr
}

impl Display for TargetParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetParseError::PortParseErr(ppe) => write!(f, "failed to parse port: {}", ppe),
            TargetParseError::ParseAddrErr => write!(f, "failed to parse addr"),
            TargetParseError::NoPortErr => write!(f, "no port was specified. ':' not present")
        }
    }
}

impl Error for TargetParseError {
    
}

impl FromStr for Target
{
    type Err = TargetParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ip, port)) = s.split_once(":") {
            let port = u16::from_str(port).map_err(|e| {TargetParseError::PortParseErr(e)})?;
            let addr = Ipv4Addr::from_str(ip).map_err(|_e| {TargetParseError::ParseAddrErr})?;
            Ok(Self {
                addr,
                port
            })
        } else {
            Err(TargetParseError::NoPortErr)
        }
    }
}

pub enum MaybeTlsStream<T> {
    Plain(TcpStream),
    Tls(T),
}

pub type ClientStream = TimedStream<MaybeTlsStream<ClientTlsStream<TcpStream>>>;
pub type ServerStream = TimedStream<MaybeTlsStream<ServerTlsStream<TcpStream>>>;

impl<T: AsyncRead + Unpin> AsyncRead for MaybeTlsStream<T> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for MaybeTlsStream<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

pub struct TimedStream<S> {
    inner: S,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    read_deadline: Option<Pin<Box<Sleep>>>,
    write_deadline: Option<Pin<Box<Sleep>>>,
}

impl<S> TimedStream<S> {
    pub fn new(inner: S, read_timeout: Option<Duration>, write_timeout: Option<Duration>) -> Self {
        Self {
            inner,
            read_timeout,
            write_timeout,
            read_deadline: None,
            write_deadline: None,
        }
    }
}

fn poll_with_deadline<T>(
    poll: Poll<io::Result<T>>,
    limit: Option<Duration>,
    deadline: &mut Option<Pin<Box<Sleep>>>,
    cx: &mut Context<'_>,
    phase: &'static str,
) -> Poll<io::Result<T>> {
    match poll {
        Poll::Ready(result) => {
            *deadline = None;
            Poll::Ready(result)
        }
        Poll::Pending => {
            if let Some(limit) = limit {
                let sleep = deadline.get_or_insert_with(|| Box::pin(sleep(limit)));
                if sleep.as_mut().poll(cx).is_ready() {
                    *deadline = None;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("{phase} timed out after {limit:?}"),
                    )));
                }
            }
            Poll::Pending
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TimedStream<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        poll_with_deadline(poll, this.read_timeout, &mut this.read_deadline, cx, "read")
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TimedStream<S> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        poll_with_deadline(poll, this.write_timeout, &mut this.write_deadline, cx, "write")
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        poll_with_deadline(poll, this.write_timeout, &mut this.write_deadline, cx, "flush")
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        poll_with_deadline(poll, this.write_timeout, &mut this.write_deadline, cx, "shutdown")
    }
}

#[derive(Clone)]
pub struct ClientTlsConfig {
    pub config: Arc<TlsClientConfig>,
    pub server_name: ServerName<'static>,
}

impl ClientTlsConfig {
    pub fn new(config: TlsClientConfig, server_name: ServerName<'static>) -> Self {
        Self {
            config: Arc::new(config),
            server_name,
        }
    }
}

#[derive(Clone)]
pub struct ClientConfig {
    pub max_message_bytes: u64,
    pub connect_timeout: Duration,
    pub tls_handshake_timeout: Duration,
    pub call_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub tls: Option<ClientTlsConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
            call_timeout: None,
            read_timeout: None,
            write_timeout: None,
            tls: None,
        }
    }
}

#[derive(Clone)]
pub struct ServerConfig {
    pub max_connections: u64,
    pub max_message_bytes: u64,
    pub idle_timeout: Duration,
    pub tls_handshake_timeout: Duration,
    pub request_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub tls: Option<Arc<TlsServerConfig>>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 0,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
            request_timeout: None,
            read_timeout: None,
            write_timeout: None,
            tls: None,
        }
    }
}

pub enum Call<C, P> {
    Single(C),
    Persistent(P)
}

impl<C: Serialize + Sync, P: Serialize + Sync> Serialize for Call<C, P> {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> std::io::Result<usize> {
        match self {
            Call::Single(c) => Ok(0u16.serialize(&mut w).await? + c.serialize(&mut w).await?),
            Call::Persistent(p) => Ok(1u16.serialize(&mut w).await? + p.serialize(&mut w).await?),
        }
    }
}

impl<C: Deserialize + Send, P: Deserialize + Send> Deserialize for Call<C, P> {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> std::io::Result<Self> {
        match u16::deserialize(&mut r).await? {
            0 => Ok(Call::Single(C::deserialize(&mut r).await?)),
            1 => Ok(Call::Persistent(P::deserialize(&mut r).await?)),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown variant tag")),
        }
    }
}

pub struct RpcClient<A: Serialize, P: Serialize = ()> {
    pub connection_target: SocketAddr,
    connection: BufStream<ClientStream>,
    config: ClientConfig,
    poisoned: bool,
    _phantom: PhantomData<(A, P)>
}

impl<A: Serialize + Sync, P: Serialize + Sync> RpcClient<A, P> {
    pub async fn new(connection_target: SocketAddr) -> RpcResult<Self> {
        Self::new_with_config(connection_target, ClientConfig::default()).await
    }

    pub async fn new_with_config(connection_target: SocketAddr, config: ClientConfig) -> RpcResult<Self> {
        let connection = establish(connection_target, &config).await?;
        Ok(Self {
            connection_target,
            connection,
            config,
            poisoned: false,
            _phantom: PhantomData,
        })
    }

    pub fn with_max_message_bytes(mut self, max_message_bytes: u64) -> Self {
        self.config.max_message_bytes = max_message_bytes;
        self
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub async fn reconnect(&mut self) -> RpcResult<()> {
        self.connection = establish(self.connection_target, &self.config).await?;
        self.poisoned = false;
        Ok(())
    }

    fn check_poisoned(&self) -> RpcResult<()> {
        if self.poisoned {
            Err(RpcError::ConnectionPoisoned)
        } else {
            Ok(())
        }
    }

    pub async fn call<R: Deserialize + std::fmt::Debug>(&mut self, action: A) -> RpcResult<R> {
        self.check_poisoned()?;
        let call_timeout = self.config.call_timeout;
        match with_timeout_opt(self.call_inner(action), "call", call_timeout).await {
            Ok(remote) => remote,
            Err(e) => {
                self.poisoned = true;
                Err(e)
            }
        }
    }

    async fn call_inner<R: Deserialize + std::fmt::Debug>(&mut self, action: A) -> RpcResult<RpcResult<R>> {
        Call::<A, P>::Single(action).serialize(&mut self.connection).await?;
        self.connection.flush().await?;
        let mut limited = (&mut self.connection).take(self.config.max_message_bytes);
        if !bool::deserialize(&mut limited).await? {
            return Err(RpcError::CallTypeRejected { reason: "call rejected by server".to_string() });
        }
        Ok(RpcResult::<R>::deserialize(&mut limited).await?)
    }

    pub async fn call_persistent<R, F>(&mut self, action: P, session: F) -> RpcResult<R>
    where
        R: Deserialize + std::fmt::Debug,
        F: AsyncFnOnce(&mut BufStream<ClientStream>) -> RpcResult<()>,
    {
        self.check_poisoned()?;
        match self.call_persistent_inner(action, session).await {
            Ok(remote) => {
                if remote.is_err() {
                    self.poisoned = true;
                }
                remote
            }
            Err(e) => {
                self.poisoned = true;
                Err(e)
            }
        }
    }

    async fn call_persistent_inner<R, F>(&mut self, action: P, session: F) -> RpcResult<RpcResult<R>>
    where
        R: Deserialize + std::fmt::Debug,
        F: AsyncFnOnce(&mut BufStream<ClientStream>) -> RpcResult<()>,
    {
        let call_timeout = self.config.call_timeout;
        let max_message_bytes = self.config.max_message_bytes;
        with_timeout_opt(async {
            Call::<A, P>::Persistent(action).serialize(&mut self.connection).await?;
            self.connection.flush().await?;
            if !bool::deserialize(&mut self.connection).await? {
                return Err(RpcError::CallTypeRejected { reason: "server has no persistence handler".to_string() });
            }
            Ok(())
        }, "persistent call setup", call_timeout).await?;
        session(&mut self.connection).await?;
        self.connection.flush().await?;
        with_timeout_opt(async {
            let mut limited = (&mut self.connection).take(max_message_bytes);
            Ok(RpcResult::<R>::deserialize(&mut limited).await?)
        }, "persistent call response", call_timeout).await
    }
}

async fn establish(target: SocketAddr, config: &ClientConfig) -> RpcResult<BufStream<ClientStream>> {
    let stream = with_timeout(TcpStream::connect(target), "TCP connect", config.connect_timeout).await?;
    let stream = match &config.tls {
        Some(tls) => {
            let connector = TlsConnector::from(tls.config.clone());
            let tls_stream = with_timeout(
                connector.connect(tls.server_name.clone(), stream),
                "TLS handshake",
                config.tls_handshake_timeout,
            ).await?;
            MaybeTlsStream::Tls(tls_stream)
        }
        None => MaybeTlsStream::Plain(stream),
    };
    Ok(BufStream::new(TimedStream::new(stream, config.read_timeout, config.write_timeout)))
}

pub trait RpcServerHandler: Send + Sync + 'static {
    type Action: Deserialize + Send + 'static;
    type Response: Serialize + Send + Sync + std::fmt::Debug + 'static;

    fn handle(&self, action: Self::Action) -> impl Future<Output = RpcResult<Self::Response>> + Send;
}

pub trait PersistentRpcServerHandler: Send + Sync + 'static {
    type Action: Deserialize + Send + 'static;
    type Response: Serialize + Send + Sync + std::fmt::Debug + 'static;

    fn handle<S: AsyncRead + AsyncWrite + Unpin + Send>(&self, action: Self::Action, s: &mut S) -> impl Future<Output = RpcResult<Self::Response>> + Send;
}

pub struct NoPersistence;

impl PersistentRpcServerHandler for NoPersistence {
    type Action = ();
    type Response = ();

    fn handle<S: AsyncRead + AsyncWrite + Unpin + Send>(&self, _action: (), _s: &mut S) -> impl Future<Output = RpcResult<()>> + Send {
        std::future::ready::<RpcResult<()>>(Err(RpcError::CallTypeRejected { reason: "no persistence handler".to_string() }))
    }
}

pub struct RpcServer<Sh: RpcServerHandler, Psh: PersistentRpcServerHandler = NoPersistence> {
    handler: Arc<Sh>,
    persistence_handler: Option<Arc<Psh>>,
    pub target: SocketAddr,
    listener: TcpListener,
    config: ServerConfig,
}

impl<Sh: RpcServerHandler> RpcServer<Sh> {
    pub async fn new(target: SocketAddr, handler: Sh) -> RpcResult<Self> {
        Ok(Self {
            handler: Arc::new(handler),
            persistence_handler: None,
            target,
            listener: TcpListener::bind(target).await?,
            config: ServerConfig::default(),
        })
    }
}

impl<Sh: RpcServerHandler, Psh: PersistentRpcServerHandler> RpcServer<Sh, Psh> {
    pub fn with_persistence<P: PersistentRpcServerHandler>(self, persistence_handler: P) -> RpcServer<Sh, P> {
        RpcServer {
            handler: self.handler,
            persistence_handler: Some(Arc::new(persistence_handler)),
            target: self.target,
            listener: self.listener,
            config: self.config,
        }
    }

    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    async fn handle_single(handler: &Sh, s: &mut BufStream<ServerStream>, action: Sh::Action) -> RpcResult<bool> {
        true.serialize(&mut *s).await?;
        let res = handler.handle(action).await;
        res.serialize(&mut *s).await?;
        s.flush().await?;
        Ok(true)
    }

    async fn handle_persistent(persistence_handler: Option<&Psh>, s: &mut BufStream<ServerStream>, action: Psh::Action, request_timeout: Option<Duration>) -> RpcResult<bool> {
        match persistence_handler {
            Some(ph) => {
                with_timeout_opt(async {
                    true.serialize(&mut *s).await?;
                    s.flush().await?;
                    Ok(())
                }, "persistent accept", request_timeout).await?;
                let res = ph.handle(action, s).await;
                let keep_alive = res.is_ok();
                with_timeout_opt(async {
                    res.serialize(&mut *s).await?;
                    s.flush().await?;
                    Ok(())
                }, "persistent response", request_timeout).await?;
                Ok(keep_alive)
            }
            None => {
                false.serialize(&mut *s).await?;
                s.flush().await?;
                Ok(false)
            }
        }
    }

    async fn incoming_handle(handler: Arc<Sh>, persistence_handler: Option<Arc<Psh>>, stream: ServerStream, max_message_bytes: u64, idle_timeout: Duration, request_timeout: Option<Duration>) {
        let mut s = BufStream::new(stream);
        loop {
            let call = match timeout(idle_timeout, Call::<Sh::Action, Psh::Action>::deserialize((&mut s).take(max_message_bytes))).await {
                Ok(Ok(call)) => call,
                _ => return,
            };
            let keep_alive = match call {
                Call::Single(action) => with_timeout_opt(Self::handle_single(&handler, &mut s, action), "request", request_timeout).await,
                Call::Persistent(action) => Self::handle_persistent(persistence_handler.as_deref(), &mut s, action, request_timeout).await,
            };
            if !matches!(keep_alive, Ok(true)) {
                return;
            }
        }
    }

    pub async fn run(&self) {
        let semaphore = if self.config.max_connections > 0 {
            Some(Arc::new(Semaphore::new(self.config.max_connections as usize)))
        } else {
            None
        };

        loop {
            let permit = match &semaphore {
                Some(sem) => match sem.clone().acquire_owned().await {
                    Ok(p) => Some(p),
                    Err(_) => return,
                },
                None => None,
            };

            let (stream, _) = match self.listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                    continue;
                }
            };

            let handler = self.handler.clone();
            let persistence_handler = self.persistence_handler.clone();
            let max_message_bytes = self.config.max_message_bytes;
            let idle_timeout = self.config.idle_timeout;
            let request_timeout = self.config.request_timeout;
            let read_timeout = self.config.read_timeout;
            let write_timeout = self.config.write_timeout;
            let tls = self.config.tls.clone();
            let tls_handshake_timeout = self.config.tls_handshake_timeout;

            tokio::spawn(async move {
                let _permit = permit;
                let stream = match tls {
                    Some(tls_config) => {
                        match with_timeout(TlsAcceptor::from(tls_config).accept(stream), "TLS handshake", tls_handshake_timeout).await {
                            Ok(s) => MaybeTlsStream::Tls(s),
                            Err(e) => {
                                eprintln!("TLS handshake failed: {:?}", e);
                                return;
                            }
                        }
                    }
                    None => MaybeTlsStream::Plain(stream),
                };
                let stream = TimedStream::new(stream, read_timeout, write_timeout);
                Self::incoming_handle(handler, persistence_handler, stream, max_message_bytes, idle_timeout, request_timeout).await;
            });
        }
    }
}

async fn with_timeout<T, E: Into<RpcError>>(
    fut: impl Future<Output = Result<T, E>>,
    phase: &'static str,
    duration: Duration,
) -> RpcResult<T> {
    match timeout(duration, fut).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err(RpcError::TimedOut {
            phase: phase.to_string(),
            after: duration,
        }),
    }
}

async fn with_timeout_opt<T>(
    fut: impl Future<Output = RpcResult<T>>,
    phase: &'static str,
    duration: Option<Duration>,
) -> RpcResult<T> {
    match duration {
        Some(d) => with_timeout(fut, phase, d).await,
        None => fut.await,
    }
}
