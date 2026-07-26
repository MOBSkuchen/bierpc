# bierpc
stands for **Bi**nary **E**ncoder and **R**emote **P**rocedure **C**aller, where the *Bi* doubles for meaning *two-way*, as **bierpc** supports client and server usage.

This repo includes *bierpc*, which is the main library including serialization and rpc logic and the *bier_derive* library, which can derive the Serialize and Deserialize trait for structs and enums.
It also includes an example in *bierpc_test*.

## Usage
The bierpc crate provides ``RpcServer`` struct, which takes an *Action* and *Return* type.
These types may be of any type that implements *Serialize* and *Deserialize* respectively
To create a new instance use:

```rust
// Target TCP addr: localhost:8000
let target = Target::from_str("127.0.0.1:5000").unwrap();
// MyHanlder is our struct that implements RpcServerHandler; look below
// SumHandler implements persistent request handling
let server = RpcServer::new(target.to_socket_addr(), MyHandler::new())
    .await.expect("Failed to bind server")
    .with_persistence(SumHandler)
    .with_config(ServerConfig { max_connections: 4, ..Default::default() });
```

We must also create a Handler struct, that implements `RpcServerHandler`.
Its method ``handle`` is called upon every call and, well, handles action and returns *something*

```rust
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
```

Additionally you may just use the 🍺 part of **bierpc**:
````rust
let mut writer = ...;
let mut reader = ...;

let my_awesome_somthing = 100u64;
handle.serialize(&mut writer);
let other_thing = String::deserialize(&mut reader);

````