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
let target = Target::new("localhost", 8000);
// handler is our struct that implements RpcServerHandler; look below
let instance = RpcServer::<Action, Return, _>::new(target, handler);
```

We must also create a Handler struct, that implements RpcServerHandler.
Its method ``handle`` is called upon every call and, well, handles action and returns *something*

```rust
impl RpcServerHandler<Action, Return> for MyHandler {
    fn handle(&self, action: Action) -> RpcResult<Return> {
        match action {
            // Example for an actions:
            Action::CreateUser(id, name) => {self.create_user(id, name)}
            Action::DeleteUser(id) => {self.delete_user(id)}
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