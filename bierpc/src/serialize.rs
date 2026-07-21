use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "generic_array_parse")]
use std::mem;
#[cfg(feature = "generic_array_parse")]
use std::mem::MaybeUninit;

pub trait Serialize {
    fn serialize<W: AsyncWrite + Unpin + Send>(&self, w: W) -> impl Future<Output = io::Result<usize>> + Send;
}

pub trait Deserialize: Sized {
    fn deserialize<R: AsyncRead + Unpin + Send>(r: R) -> impl Future<Output = io::Result<Self>> + Send;
}

pub const fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

pub const fn type_hash(name: &str) -> u64 {
    fnv1a_64(name.as_bytes())
}

pub const fn combine_type_hashes(base: u64, other: u64) -> u64 {
    let bytes = other.to_be_bytes();
    let mut hash = base;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

pub trait SerializeVerified: Serialize + Sync {
    const TYPE_HASH: u64;

    fn serialize_verified<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> impl Future<Output = io::Result<usize>> + Send {
        async move {
            let mut total = Self::TYPE_HASH.serialize(&mut w).await?;
            total += self.serialize(&mut w).await?;
            Ok(total)
        }
    }
}

pub trait DeserializeVerified: Deserialize {
    const TYPE_HASH: u64;

    fn deserialize_verified<R: AsyncRead + Unpin + Send>(mut r: R) -> impl Future<Output = io::Result<Option<Self>>> + Send {
        async move {
            let hash = u64::deserialize(&mut r).await?;
            if hash != Self::TYPE_HASH {
                return Ok(None);
            }
            Ok(Some(Self::deserialize(&mut r).await?))
        }
    }
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

macro_rules! impl_verified {
    ($($t:ty),*) => {
        $(
            impl SerializeVerified for $t {
                const TYPE_HASH: u64 = type_hash(stringify!($t));
            }

            impl DeserializeVerified for $t {
                const TYPE_HASH: u64 = type_hash(stringify!($t));
            }
        )*
    };
}

impl_verified!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool, String, PathBuf, SocketAddr);

macro_rules! impl_tuples {
    ( $( $name:ident )+ ) => {
        impl<$($name: Sync + Send + Serialize),+> Serialize for ($($name,)+) {
            async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
                let ($($name,)+) = self;

                let mut total_bytes = 0;

                $(
                    total_bytes += $name.serialize(&mut w).await?;
                )+

                Ok(total_bytes)
            }
        }

        impl<$($name: Sync + Send + Deserialize),+> Deserialize for ($($name,)+) {
            async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
                Ok((
                    $(
                        $name::deserialize(&mut r).await?,
                    )+
                ))
            }
        }

        impl<$($name: SerializeVerified + Send),+> SerializeVerified for ($($name,)+) {
            const TYPE_HASH: u64 = {
                let mut hash = type_hash("tuple");
                $( hash = combine_type_hashes(hash, $name::TYPE_HASH); )+
                hash
            };
        }

        impl<$($name: DeserializeVerified + Sync + Send),+> DeserializeVerified for ($($name,)+) {
            const TYPE_HASH: u64 = {
                let mut hash = type_hash("tuple");
                $( hash = combine_type_hashes(hash, $name::TYPE_HASH); )+
                hash
            };
        }
    };
}

impl_tuples! { a }
impl_tuples! { a b }
impl_tuples! { a b c }

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

impl<T: SerializeVerified> SerializeVerified for Option<T> {
    const TYPE_HASH: u64 = combine_type_hashes(type_hash("Option"), T::TYPE_HASH);
}

impl<T: DeserializeVerified> DeserializeVerified for Option<T> {
    const TYPE_HASH: u64 = combine_type_hashes(type_hash("Option"), T::TYPE_HASH);
}

impl<T: Serialize + Sync> Serialize for Vec<T> {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let mut total = (self.len() as u64).serialize(&mut w).await?;
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
        let mut i = 0;
        while i < len {
            out.insert(i, T::deserialize(&mut r).await?);
            i += 1;
        }
        Ok(out)
    }
}

impl<T: SerializeVerified> SerializeVerified for Vec<T> {
    const TYPE_HASH: u64 = combine_type_hashes(type_hash("Vec"), T::TYPE_HASH);
}

impl<T: DeserializeVerified + Send> DeserializeVerified for Vec<T> {
    const TYPE_HASH: u64 = combine_type_hashes(type_hash("Vec"), T::TYPE_HASH);
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


impl<T: SerializeVerified + std::fmt::Debug, E: SerializeVerified + std::fmt::Debug> SerializeVerified for Result<T, E> {
    const TYPE_HASH: u64 = combine_type_hashes(combine_type_hashes(type_hash("Result"), T::TYPE_HASH), E::TYPE_HASH);
}

impl<T: DeserializeVerified + std::fmt::Debug, E: DeserializeVerified + std::fmt::Debug> DeserializeVerified for Result<T, E> {
    const TYPE_HASH: u64 = combine_type_hashes(combine_type_hashes(type_hash("Result"), T::TYPE_HASH), E::TYPE_HASH);
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

#[cfg(feature = "generic_array_parse")]
impl<T, const L: usize> Serialize for [T; L]
where T: Serialize + Sync
{
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let mut t = (L as u64).serialize(&mut w).await?;
        for i in self {
            t += i.serialize(&mut w).await?;
        }
        Ok(t)
    }
}

#[cfg(feature = "generic_array_parse")]
impl<T, const L: usize> Deserialize for [T; L]
where T: Deserialize + Sync + Sized + Send
{
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let len = u64::deserialize(&mut r).await? as usize;
        if len != L {
            return Err(io::Error::new(ErrorKind::InvalidData, "Reported size is not expected size"))
        }
        let mut data: [MaybeUninit<T>; L] = [const { MaybeUninit::uninit() }; L];
        for i in 0..len {
            let d = T::deserialize(&mut r).await?;
            data[i].write(d);
        }
        unsafe {
            let fully_initialized_array = mem::transmute_copy(&data);
            mem::forget(data);
            Ok(fully_initialized_array)
        }
    }
}

#[cfg(feature = "generic_array_parse")]
impl<T: SerializeVerified, const L: usize> SerializeVerified for [T; L] {
    const TYPE_HASH: u64 = combine_type_hashes(combine_type_hashes(type_hash("array"), L as u64), T::TYPE_HASH);
}

#[cfg(feature = "generic_array_parse")]
impl<T: DeserializeVerified + Sync + Send, const L: usize> DeserializeVerified for [T; L] {
    const TYPE_HASH: u64 = combine_type_hashes(combine_type_hashes(type_hash("array"), L as u64), T::TYPE_HASH);
}

impl<K: Serialize + Sync, V: Serialize + Sync> Serialize for HashMap<K, V> {
    async fn serialize<W: AsyncWrite + Unpin + Send>(&self, mut w: W) -> io::Result<usize> {
        let mut total = (self.len() as u64).serialize(&mut w).await?;
        for (k, v) in self {
            total += k.serialize(&mut w).await?;
            total += v.serialize(&mut w).await?;
        }
        Ok(total)
    }
}

impl<K: Deserialize + std::hash::Hash + std::cmp::Eq + Send, V: Deserialize + Send> Deserialize for HashMap<K, V> {
    async fn deserialize<R: AsyncRead + Unpin + Send>(mut r: R) -> io::Result<Self> {
        let len = u64::deserialize(&mut r).await?;
        let mut hashmap = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            hashmap.insert(K::deserialize(&mut r).await?, V::deserialize(&mut r).await?);
        }
        Ok(hashmap)
    }
}

impl<K: SerializeVerified, V: SerializeVerified> SerializeVerified for HashMap<K, V> {
    const TYPE_HASH: u64 = combine_type_hashes(combine_type_hashes(type_hash("HashMap"), K::TYPE_HASH), V::TYPE_HASH);
}

impl<K: DeserializeVerified + std::hash::Hash + std::cmp::Eq + Send, V: DeserializeVerified + Send> DeserializeVerified for HashMap<K, V> {
    const TYPE_HASH: u64 = combine_type_hashes(combine_type_hashes(type_hash("HashMap"), K::TYPE_HASH), V::TYPE_HASH);
}