pub mod serialize;
pub mod error;

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use crate::error::{RpcError, RpcResult};
use crate::serialize::{Deserialize, Serialize};

pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1024 * 1024;
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Target {
    port: u16,
    addr: String
}

impl Target {
    pub fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr.parse().unwrap(), self.port)
    }

    pub fn new(addr: String, port: u16) -> Self {
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
    connection: BufStream<TcpStream>,
    max_message_bytes: u64,
    poisoned: bool,
    _phantom: PhantomData<(A, P)>
}

impl<A: Serialize + Sync, P: Serialize + Sync> RpcClient<A, P> {
    pub async fn new(connection_target: SocketAddr) -> RpcResult<Self> {
        let connection = TcpStream::connect(connection_target).await?;
        Ok(Self {
            connection_target,
            connection: BufStream::new(connection),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            poisoned: false,
            _phantom: PhantomData,
        })
    }

    pub fn with_max_message_bytes(mut self, max_message_bytes: u64) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub async fn reconnect(&mut self) -> RpcResult<()> {
        self.connection = BufStream::new(TcpStream::connect(self.connection_target).await?);
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
        match self.call_inner(action).await {
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
        let mut limited = (&mut self.connection).take(self.max_message_bytes);
        if !bool::deserialize(&mut limited).await? {
            return Err(RpcError::CallTypeRejected { reason: "call rejected by server".to_string() });
        }
        Ok(RpcResult::<R>::deserialize(&mut limited).await?)
    }

    pub async fn call_persistent<R, F>(&mut self, action: P, session: F) -> RpcResult<R>
    where
        R: Deserialize + std::fmt::Debug,
        F: AsyncFnOnce(&mut BufStream<TcpStream>) -> RpcResult<()>,
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
        F: AsyncFnOnce(&mut BufStream<TcpStream>) -> RpcResult<()>,
    {
        Call::<A, P>::Persistent(action).serialize(&mut self.connection).await?;
        self.connection.flush().await?;
        if !bool::deserialize(&mut self.connection).await? {
            return Err(RpcError::CallTypeRejected { reason: "server has no persistence handler".to_string() });
        }
        session(&mut self.connection).await?;
        self.connection.flush().await?;
        let mut limited = (&mut self.connection).take(self.max_message_bytes);
        Ok(RpcResult::<R>::deserialize(&mut limited).await?)
    }
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

pub struct ServerConfig {
    pub max_connections: u64,
    pub max_message_bytes: u64,
    pub idle_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 0,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
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

    async fn handle_single(handler: &Sh, s: &mut BufStream<TcpStream>, action: Sh::Action) -> RpcResult<bool> {
        true.serialize(&mut *s).await?;
        let res = handler.handle(action).await;
        res.serialize(&mut *s).await?;
        s.flush().await?;
        Ok(true)
    }

    async fn handle_persistent(persistence_handler: Option<&Psh>, s: &mut BufStream<TcpStream>, action: Psh::Action) -> RpcResult<bool> {
        match persistence_handler {
            Some(ph) => {
                true.serialize(&mut *s).await?;
                s.flush().await?;
                let res = ph.handle(action, s).await;
                let keep_alive = res.is_ok();
                res.serialize(&mut *s).await?;
                s.flush().await?;
                Ok(keep_alive)
            }
            None => {
                false.serialize(&mut *s).await?;
                s.flush().await?;
                Ok(false)
            }
        }
    }

    async fn incoming_handle(handler: Arc<Sh>, persistence_handler: Option<Arc<Psh>>, stream: TcpStream, max_message_bytes: u64, idle_timeout: Duration) {
        let mut s = BufStream::new(stream);
        loop {
            let call = match timeout(idle_timeout, Call::<Sh::Action, Psh::Action>::deserialize((&mut s).take(max_message_bytes))).await {
                Ok(Ok(call)) => call,
                _ => return,
            };
            let keep_alive = match call {
                Call::Single(action) => Self::handle_single(&handler, &mut s, action).await,
                Call::Persistent(action) => Self::handle_persistent(persistence_handler.as_deref(), &mut s, action).await,
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

            tokio::spawn(async move {
                let _permit = permit;
                Self::incoming_handle(handler, persistence_handler, stream, max_message_bytes, idle_timeout).await;
            });
        }
    }
}
