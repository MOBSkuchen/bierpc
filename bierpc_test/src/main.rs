use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bierpc::serialize::Serialize;
use bierpc::serialize::Deserialize;
use std::thread;
use bierpc::{RpcClient, RpcServer, RpcServerHandler, Target};
use bierpc::error::RpcResult;
use bier_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
enum Action {
    CreateUser(u64, String),
    DeleteUser(u64)
}

#[derive(Serialize, Deserialize)]
#[derive(Debug)]
enum Return {
    CreateUserSuccess(u64),
    DeleteUserSuccess(u64)
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
    fn handle(&self, action: Action) -> RpcResult<MyDummyResult> {
        match action {
            Action::CreateUser(id, name) => {self.create_user(id, name)}
            Action::DeleteUser(id) => {self.delete_user(id)}
        }
    }
}

fn main() {
    let port = 8080;
    let target = Target::new("127.0.0.1".to_string(), port);

    let server_target = target.clone();
    thread::spawn(move || {
        let handler = MyHandler::new();
        let server = RpcServer::<Action, MyDummyResult, _>::new(server_target, handler)
            .expect("Failed to start server");
        server.run(4);
    });

    thread::sleep(std::time::Duration::from_millis(100));

    println!("[Client] Connecting...");
    let mut client = RpcClient::<Action>::new(target)
        .expect("Failed to create client");

    let input = Action::DeleteUser(10);
    println!("[Client] Sending: \"{:?}\"", input);

    match client.call::<MyDummyResult>(input) {
        Ok(response) => {
            println!("[Client] Received: \"{:?}\"", response);
            println!("[Test] SUCCESS: String was reversed correctly.");
        },
        Err(e) => {
            println!("[Test] FAILED: {:?}", e);
        }
    }
}