pub mod serialize;
pub mod error;

use std::marker::PhantomData;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use crate::error::RpcResult;
use crate::serialize::{Deserialize, Serialize};

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

pub struct RpcClient<A: Serialize> {
    pub connection_target: SocketAddr,
    connection: TcpStream,
    _phantom1: PhantomData<A>
}

impl<A: Serialize> RpcClient<A> {
    pub async fn new(connection_target: SocketAddr) -> RpcResult<Self> {
        let connection = TcpStream::connect(connection_target).await?;
        Ok(Self {
            connection_target,
            connection,
            _phantom1: PhantomData,
        })
    }

    pub async fn call<R: Deserialize>(&mut self, action: A) -> RpcResult<R> {
        action.serialize(&mut self.connection).await?;
        use tokio::io::AsyncWriteExt;
        self.connection.flush().await?;

        Ok(R::deserialize(&mut self.connection).await?)
    }
}

pub trait RpcServerHandler<A: Deserialize, R: Serialize>: Send + Sync + 'static {
    fn handle(&self, action: A) -> impl Future<Output = RpcResult<R>> + Send;
}

pub struct RpcServer<A: Deserialize, R: Serialize, Psh: RpcServerHandler<A, R>> {
    handler: Arc<Psh>,
    pub target: SocketAddr,
    listener: TcpListener,
    _phantom1: PhantomData<A>,
    _phantom2: PhantomData<R>,
}

impl<
    A: Deserialize + Send + Sync + std::fmt::Debug + 'static,
    R: Serialize + Send + Sync + std::fmt::Debug + 'static,
    Psh: RpcServerHandler<A, R>
> RpcServer<A, R, Psh> {

    pub async fn new(target: SocketAddr, handler: Psh) -> RpcResult<Self> {
        let listener = TcpListener::bind(target).await?;
        Ok(Self {
            handler: Arc::new(handler),
            target,
            listener,
            _phantom1: PhantomData,
            _phantom2: PhantomData
        })
    }

    async fn incoming_handle(handler: Arc<Psh>, mut s: TcpStream) {
        let res: RpcResult<()> = async {
            let action = A::deserialize(&mut s).await?;
            let res = handler.handle(action).await?;
            res.serialize(&mut s).await?;

            use tokio::io::AsyncWriteExt;
            s.flush().await?;
            Ok(())
        }.await;

        if let Err(e) = res {
            eprintln!("RPC Error: {:?}", e);
        }
    }

    pub async fn run(&self, max_cons: u64) {
        /*
            If max_cons > 0, we use a Semaphore to limit concurrency.
            If 0, we assume unbounded (or strictly 1 if mimicking original logic literally,
            but unbounded is usually preferred for 0 in async contexts.
            Here we'll treat 0 as "no limit").
        */
        let semaphore = if max_cons > 0 {
            Some(Arc::new(Semaphore::new(max_cons as usize)))
        } else {
            None
        };

        loop {
            // Accept new connection
            let (stream, _) = match self.listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                    continue;
                }
            };

            let handler = self.handler.clone();
            let sem_clone = semaphore.clone();

            tokio::spawn(async move {
                let _permit = if let Some(sem) = sem_clone {
                    match sem.acquire_owned().await {
                        Ok(p) => Some(p),
                        Err(_) => return, // Semaphore closed
                    }
                } else {
                    None
                };

                Self::incoming_handle(handler, stream).await;
            });
        }
    }
}