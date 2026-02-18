pub mod serialize;
pub mod error;

use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use crate::error::RpcResult;
use crate::serialize::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Target {
    port: u16,
    addr: String
}

impl Target {
    fn to_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr.parse().unwrap(), self.port)
    }

    pub fn new(addr: String, port: u16) -> Self {
        Self {
            addr,
            port
        }
    }
}

pub struct RpcClient<A: Serialize> {
    pub connection_target: Target,
    connection: TcpStream,
    _phantom1: PhantomData<A>
}

impl<A: Serialize> RpcClient<A> {
    pub fn new(connection_target: Target) -> RpcResult<Self> {
        let connection = TcpStream::connect(connection_target.to_socket_addr())?;
        Ok(Self {
            connection_target,
            connection,
            _phantom1: PhantomData,
        })
    }

    pub fn call<R: Deserialize>(&mut self, action: A) -> RpcResult<R> {
        action.serialize(&mut self.connection)?;
        Ok(R::deserialize(&mut self.connection)?)
    }
}

pub trait RpcServerHandler<A: Deserialize, R: Serialize> {
    fn handle(&self, action: A) -> RpcResult<R>;
}

pub struct RpcServer<A: Deserialize, R: Serialize, Psh: RpcServerHandler<A, R>> {
    handler: Psh,
    pub target: Target,
    listener: TcpListener,
    _phantom1: PhantomData<A>,
    _phantom2: PhantomData<R>,
}

impl<A: Deserialize + Sync + std::fmt::Debug, R: Serialize + Sync + std::fmt::Debug, Psh: RpcServerHandler<A, R> + Sync> RpcServer<A, R, Psh> {
    pub fn new(target: Target, handler: Psh) -> RpcResult<Self> {
        let listener = TcpListener::bind(target.to_socket_addr())?;
        Ok(Self {
            handler,
            target,
            listener,
            _phantom1: PhantomData,
            _phantom2: PhantomData
        })
    }

    fn incoming_handle(&self, mut s: TcpStream) {
        let res: RpcResult<()> = (|| {
            let action = A::deserialize(&mut s)?;
            let res = self.handler.handle(action)?;
            res.serialize(&mut s)?;
            Ok(())
        })();
        if let Err(e) = res {
            e.serialize(s).expect("Damn");
        }
    }

    pub fn run(&self, max_cons: u64) {
        let pool_size = if max_cons == 0 { 1 } else { max_cons as usize };

        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::sync_channel::<TcpStream>(0);

            let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

            for _ in 0..pool_size {
                let rx = rx.clone();
                scope.spawn(move || {
                    loop {
                        let stream = {
                            let lock = rx.lock().unwrap();
                            match lock.recv() {
                                Ok(s) => s,
                                Err(_) => break, // Channel closed, shut down worker
                            }
                        };

                        self.incoming_handle(stream);
                    }
                });
            }

            for stream in self.listener.incoming() {
                match stream {
                    Ok(s) => {
                        if let Err(e) = tx.send(s) {
                            eprintln!("Failed to send stream to worker (server shutting down?): {}", e);
                            break;
                        }
                    }
                    Err(e) => eprintln!("Connection failed: {}", e),
                }
            }
        });
    }
}