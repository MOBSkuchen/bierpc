use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use bierpc::serialize::{Serialize, Deserialize};
use bierpc::{RpcClient, RpcServer, RpcServerHandler, Target};
use bierpc::error::RpcResult;
use bier_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
enum Action {
    CreateUser(u64, String),
    DeleteUser(u64),
    DeleteUser2([u8; 8])
}

#[derive(Serialize, Deserialize, Debug)]
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

impl RpcServerHandler<Action, MyDummyResult> for MyHandler {
    async fn handle(&self, action: Action) -> RpcResult<MyDummyResult> {
        match action {
            Action::CreateUser(id, name) => self.create_user(id, name),
            Action::DeleteUser(id) => self.delete_user(id),
            Action::DeleteUser2(id) => self.delete_user(u64::from_le_bytes(id))
        }
    }
}

#[tokio::main]
async fn main() {
    let port = 8080;
    let target = Target::new("127.0.0.1".to_string(), port);

    let server_target = target.clone();

    // Spawn the server as a Tokio background task
    tokio::spawn(async move {
        let handler = MyHandler::new();
        let server = RpcServer::<Action, MyDummyResult, _>::new(server_target, handler)
            .await
            .expect("Failed to bind server");

        // run() is now async and infinite
        server.run(4).await;
    });

    sleep(Duration::from_millis(100)).await;

    println!("[Client] Connecting...");
    let mut client = RpcClient::<Action>::new(target)
        .await
        .expect("Failed to create client");

    let input = Action::DeleteUser2([0u8; 8]);
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
}