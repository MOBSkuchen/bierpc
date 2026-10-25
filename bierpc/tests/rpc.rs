use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncWrite};
use bierpc::serialize::{Serialize, Deserialize};
use bierpc::rpc::{PersistentRpcServerHandler, RpcClient, RpcServer, RpcServerHandler, Target};
use bierpc::error::RpcResult;
use bier_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
enum Action {
    Echo(String),
    Add { a: u64, b: u64 },
}

struct MyHandler;

impl RpcServerHandler for MyHandler {
    type Action = Action;
    type Response = String;

    async fn handle(&self, action: Action) -> RpcResult<String> {
        match action {
            Action::Echo(s) => Ok(s),
            Action::Add { a, b } => Ok((a + b).to_string()),
        }
    }
}

struct SumHandler;

impl PersistentRpcServerHandler for SumHandler {
    type Action = u64;
    type Response = u64;

    async fn handle<S: AsyncRead + AsyncWrite + Unpin + Send>(&self, count: u64, s: &mut S) -> RpcResult<u64> {
        let mut sum = 0u64;
        for _ in 0..count {
            sum += u64::deserialize(&mut *s).await?;
        }
        Ok(sum)
    }
}

#[tokio::test]
async fn call_and_persistent_call() {
    let target = Target::from_str("127.0.0.1:0").unwrap();
    let server = RpcServer::new(target.to_socket_addr(), MyHandler)
        .await
        .expect("Failed to bind server")
        .with_persistence(SumHandler);
    let addr = server.local_addr().unwrap();

    tokio::spawn(async move { server.run().await });

    let mut client = RpcClient::<Action, u64>::new(addr)
        .await
        .expect("Failed to create client");

    let echoed = client.call::<String>(Action::Echo("Hi!".to_string())).await.unwrap();
    assert_eq!(echoed, "Hi!");

    let sum = client.call_persistent::<u64, _>(3, async |s| {
        for n in [1u64, 2, 3] {
            n.serialize(&mut *s).await?;
        }
        Ok(())
    }).await.unwrap();
    assert_eq!(sum, 6);

    // Same connection still usable after a persistent session
    let added = client.call::<String>(Action::Add { a: 2, b: 3 }).await.unwrap();
    assert_eq!(added, "5");
}
