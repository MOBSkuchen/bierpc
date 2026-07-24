use std::io;
use crate::serialize::{Serialize, Deserialize};
use bier_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub enum RpcError {
    IoError {
        err: io::Error
    },
    CallTypeRejected {
        reason: String
    },
    ConnectionPoisoned
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> Self {
        RpcError::IoError { err }
    }
}

pub type RpcResult<T> = Result<T, RpcError>;