use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{sleep, Duration};
use bierpc::serialize::{Serialize, Deserialize, SerializeVerified, DeserializeVerified};
use bierpc::rpc::{PersistentRpcServerHandler, RpcClient, RpcServer, RpcServerHandler, ServerConfig, Target};
use bierpc::error::RpcResult;
use bier_derive::{Serialize, Deserialize, SerializeVerified, DeserializeVerified};

#[derive(Serialize, Deserialize, SerializeVerified, DeserializeVerified, Debug)]
enum Action {
    CreateUser { id: u64, name: String },
    DeleteUser(u64),
    DeleteUser2([u8; 8])
}

#[derive(Serialize, Deserialize, SerializeVerified, DeserializeVerified, Debug)]
enum Return {
    CreateUserSuccess(u64),
    DeleteUserSuccess(u64),
}

type MyDummyResult = Result<Return, String>;

struct MyHandler {
    users: Arc<Mutex<HashMap<u64, String>>>
}

impl MyHandler {
    pub fn new() -> Self {
        Self {
            users: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    fn create_user(&self, id: u64, name: String) -> RpcResult<MyDummyResult> {
        self.users.lock().unwrap().insert(id, name);
        Ok(Ok(Return::CreateUserSuccess(id)))
    }

    fn delete_user(&self, id: u64) -> RpcResult<MyDummyResult> {
        self.users.lock().unwrap().remove(&id);
        Ok(Ok(Return::DeleteUserSuccess(id)))
    }
}

impl RpcServerHandler for MyHandler {
    type Action = Action;
    type Response = MyDummyResult;

    async fn handle(&self, action: Action) -> RpcResult<MyDummyResult> {
        match action {
            Action::CreateUser { id, name } => self.create_user(id, name),
            Action::DeleteUser(id) => self.delete_user(id),
            Action::DeleteUser2(id) => self.delete_user(u64::from_le_bytes(id))
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

#[tokio::main]
async fn main() {
    let port = 8080;
    let target = Target::new("127.0.0.1".to_string(), port);

    let server_target = target.clone();

    tokio::spawn(async move {
        let server = RpcServer::new(server_target.to_socket_addr().unwrap(), MyHandler::new())
            .await
            .expect("Failed to bind server")
            .with_persistence(SumHandler)
            .with_config(ServerConfig { max_connections: 4, ..Default::default() });

        server.run().await;
    });

    sleep(Duration::from_millis(100)).await;

    println!("[Client] Connecting...");
    let mut client = RpcClient::<Action, u64>::new(target.to_socket_addr().unwrap())
        .await
        .expect("Failed to create client");

    let input = Action::CreateUser { id: 1, name: "Hi!".to_string() };
    println!("[Client] Sending: \"{:?}\"", input);

    match client.call::<MyDummyResult>(input).await {
        Ok(response) => {
            println!("[Client] Received: \"{:?}\"", response);
            println!("[Test] SUCCESS");
        },
        Err(e) => {
            println!("[Test] FAILED: {:?}", e);
        }
    }

    match client.call_persistent::<u64, _>(3, async |s| {
        for n in [1u64, 2, 3] {
            n.serialize(&mut *s).await?;
        }
        Ok(())
    }).await {
        Ok(sum) => println!("[Persistent] Sum: {} (expected 6)", sum),
        Err(e) => println!("[Persistent] FAILED: {:?}", e),
    }

    let follow_up = Action::DeleteUser(1);
    println!("[Client] Sending follow-up on same connection: \"{:?}\"", follow_up);
    match client.call::<MyDummyResult>(follow_up).await {
        Ok(response) => println!("[Client] Received: \"{:?}\"", response),
        Err(e) => println!("[Test] FAILED: {:?}", e),
    }

    let (mut tx, mut rx) = tokio::io::duplex(1024);

    let verified_input = Action::CreateUser { id: 2, name: "Verified".to_string() };
    println!("[Verified] Sending: \"{:?}\"", verified_input);
    verified_input.serialize_verified(&mut tx).await.expect("Failed to serialize");

    match Action::deserialize_verified(&mut rx).await.expect("Failed to deserialize") {
        Some(action) => println!("[Verified] Received: \"{:?}\"", action),
        None => println!("[Verified] FAILED: unexpected hash mismatch"),
    }

    let verified_input = Action::DeleteUser(2);
    verified_input.serialize_verified(&mut tx).await.expect("Failed to serialize");

    match Return::deserialize_verified(&mut rx).await.expect("Failed to deserialize") {
        Some(ret) => println!("[Verified] FAILED: expected hash mismatch, got \"{:?}\"", ret),
        None => println!("[Verified] Hash mismatch detected as expected"),
    }
}
