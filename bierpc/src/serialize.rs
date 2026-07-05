use std::ffi::OsStr;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub trait Serialize {
    fn serialize<W: AsyncWrite + Unpin + Send>(&self, w: W) -> impl Future<Output = io::Result<usize>> + Send;
}

pub trait Deserialize: Sized {
    fn deserialize<R: AsyncRead + Unpin + Send>(r: R) -> impl Future<Output = io::Result<Self>> + Send;
}

macro_rules! impl_serialization {
    ($($t:ty),*) => {
        $(
            impl Serialize for $t {
                async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
                    let bytes = self.to_be_bytes();
                    w.write_all(&bytes).await?;
                    Ok(bytes.len())
                }
            }

            impl Deserialize for $t {
                async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
                    let mut buf = [0u8; std::mem::size_of::<$t>()];
                    r.read_exact(&mut buf).await?;
                    Ok(Self::from_be_bytes(buf))
                }
            }
        )*
    };
}

impl_serialization!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64);

impl Serialize for bool {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, w: W) -> io::Result<usize> {
        (*self as u8).serialize(w).await
    }
}

impl Deserialize for bool {
    async fn deserialize<R: AsyncRead + Unpin + Send>(r: R) -> io::Result<Self> {
        let val = u8::deserialize(r).await?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid bool value")),
        }
    }
}

impl Serialize for String {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let bytes = self.as_bytes();
        let len = bytes.len() as u32;

        // Note the &mut w passed here. Since W is Unpin, &mut W implements AsyncWrite.
        let mut written = len.serialize(&mut w).await?;
        w.write_all(bytes).await?;
        written += bytes.len();

        Ok(written)
    }
}

impl Deserialize for String {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let len = u32::deserialize(&mut r).await? as usize;
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf).await?;

        String::from_utf8(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl<T: Serialize + Sync> Serialize for Option<T> {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        match self {
            Some(val) => {
                let mut written = 1u8.serialize(&mut w).await?;
                written += val.serialize(w).await?;
                Ok(written)
            }
            None => {
                0u8.serialize(w).await
            }
        }
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let tag = u8::deserialize(&mut r).await?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::deserialize(r).await?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Option tag")),
        }
    }
}

impl<T: Serialize + Sync> Serialize for Vec<T> {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        (self.len() as u64).serialize(&mut w).await?;
        let mut total = 8;
        for i in self {
            total += i.serialize(&mut w).await?;
        }
        Ok(total)
    }
}

impl<T: Deserialize + Send> Deserialize for Vec<T> {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let len = u64::deserialize(&mut r).await? as usize;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(T::deserialize(&mut r).await?);
        }
        Ok(out)
    }
}

impl<T: Serialize + std::fmt::Debug + Sync, E: Serialize + std::fmt::Debug + Sync> Serialize for Result<T, E> {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let mut total = self.is_ok().serialize(&mut w).await?;

        total += match self {
            Ok(x) => x.serialize(&mut w).await?,
            Err(e) => e.serialize(&mut w).await?
        };

        Ok(total)
    }
}

impl<T: Deserialize + std::fmt::Debug, E: Deserialize + std::fmt::Debug> Deserialize for Result<T, E> {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        if bool::deserialize(&mut r).await? {
            Ok(Ok(T::deserialize(&mut r).await?))
        } else {
            Ok(Err(E::deserialize(&mut r).await?))
        }
    }
}


impl Serialize for PathBuf {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let s = self.as_os_str().to_str().ok_or(io::Error::new(ErrorKind::InvalidData, "Could not convert path to UTF-8"))?.to_string();
        s.serialize(&mut w).await
    }
}

impl Deserialize for PathBuf {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let s = String::deserialize(&mut r).await?;
        PathBuf::from_str(s.as_str()).map_err(|e| {io::Error::new(ErrorKind::InvalidData, "Could not convert bare string to PathBuf")})
    }
}

impl Serialize for SocketAddr {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let mut t = self.is_ipv4().serialize(&mut w).await?;
        t += self.port().serialize(&mut w).await?;
        t += match self.ip() {
            IpAddr::V4(ip) => {
                ip.to_bits().serialize(&mut w).await?
            }
            IpAddr::V6(ip) => {
                ip.to_bits().serialize(&mut w).await?
            }
        };
        Ok(t)
    }
}

impl Deserialize for SocketAddr {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let is_ipv4 = bool::deserialize(&mut r).await?;
        let port = u16::deserialize(&mut r).await?;
        let ip = if is_ipv4 {
            IpAddr::V4(Ipv4Addr::from_bits(u32::deserialize(&mut r).await?))
        } else {
            IpAddr::V6(Ipv6Addr::from_bits(u128::deserialize(&mut r).await?))
        };
        Ok(SocketAddr::new(ip, port))
    }
}
