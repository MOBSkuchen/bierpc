use std::io;
use crate::serialize::{Serialize, Deserialize};
use bier_derive::{Deserialize, Serialize};

// Dead wrapper for an IO Error. Might be expanded in the future
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub enum RpcError {
    IoError,
}

impl From<io::Error> for RpcError {
    fn from(_e: io::Error) -> Self {
        RpcError::IoError
    }
}

pub type RpcResult<T> = Result<T, RpcError>;