use std::hash::{Hash, Hasher};

use ahash::AHasher;
use pingora::{
    protocols::{l4::socket::SocketAddr, ALPN},
    upstreams::peer::{Peer, PeerOptions, Proxy},
};

#[derive(Debug, Clone)]
pub struct MixedPeer {
    pub _address: SocketAddr,
    pub transport_type: TransportType,
    pub sni: String,
    pub proxy: Option<Proxy>,
    pub reuse: bool,

    pub options: PeerOptions,
}

impl MixedPeer {
    pub fn new(
        address: SocketAddr,
        transport_type: TransportType,
        sni: String,
        skip_verify: bool,
        reuse: bool,
    ) -> Self {
        let mut options = PeerOptions::new();
        if skip_verify {
            options.verify_cert = false;
            options.verify_hostname = false;
        }
        options.alpn = ALPN::H2;

        Self {
            _address: address,
            transport_type,
            sni,
            proxy: None,
            reuse,
            options,
        }
    }

    pub fn new_tcp(address: SocketAddr, reuse: bool) -> Self {
        Self::new(address, TransportType::Tcp, "".to_string(), false, reuse)
    }

    pub fn new_tls(address: SocketAddr, sni: String, skip_verify: bool, reuse: bool) -> Self {
        Self::new(address, TransportType::Tls, sni, skip_verify, reuse)
    }

    pub fn new_http(address: SocketAddr, sni: String, reuse: bool) -> Self {
        Self::new(address, TransportType::Http, sni, false, reuse)
    }

    pub fn new_https(address: SocketAddr, sni: String, skip_verify: bool, reuse: bool) -> Self {
        Self::new(address, TransportType::Https, sni, skip_verify, reuse)
    }
}

impl Peer for MixedPeer {
    fn address(&self) -> &pingora::protocols::l4::socket::SocketAddr {
        &self._address
    }

    fn tls(&self) -> bool {
        matches!(
            self.transport_type,
            TransportType::Tls | TransportType::Https
        )
    }

    fn sni(&self) -> &str {
        &self.sni
    }

    fn reuse_hash(&self) -> u64 {
        let mut hasher = AHasher::default();
        self.hash(&mut hasher);
        hasher.finish()
    }

    fn get_peer_options(&self) -> Option<&PeerOptions> {
        Some(&self.options)
    }

    fn get_mut_peer_options(&mut self) -> Option<&mut PeerOptions> {
        Some(&mut self.options)
    }

    fn get_proxy(&self) -> Option<&Proxy> {
        self.proxy.as_ref()
    }
}

impl std::fmt::Display for MixedPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "addr: {}, transport_type: {}",
            self._address, self.transport_type
        )?;
        if !self.sni.is_empty() {
            write!(f, ", sni: {}", self.sni)?;
        }
        if let Some(proxy) = &self.proxy {
            write!(f, ", proxy: {}", proxy)?;
        }
        Ok(())
    }
}

impl std::hash::Hash for MixedPeer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self._address.hash(state);
        self.transport_type.hash(state);
        self.proxy.hash(state);
        self.sni.hash(state);
        self.verify_cert().hash(state);
        self.verify_hostname().hash(state);
        self.alternative_cn().hash(state);
        self.reuse.hash(state);
    }
}

#[derive(Debug, Clone, Hash)]
pub enum TransportType {
    Tcp,
    Tls,
    Http,
    Https,
}

impl std::str::FromStr for TransportType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tcp" => Ok(Self::Tcp),
            "tls" => Ok(Self::Tls),
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            _ => Err(format!("Invalid variant: {s}")),
        }
    }
}

impl std::fmt::Display for TransportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp => write!(f, "TCP"),
            Self::Tls => write!(f, "TLS"),
            Self::Http => write!(f, "HTTP"),
            Self::Https => write!(f, "HTTPS"),
        }
    }
}
