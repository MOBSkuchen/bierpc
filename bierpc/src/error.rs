use std::io;
use std::time::Duration;
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
    ConnectionPoisoned,
    TimedOut {
        phase: String,
        after: Duration
    }
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> Self {
        RpcError::IoError { err }
    }
}

pub type RpcResult<T> = Result<T, RpcError>;