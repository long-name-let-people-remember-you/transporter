use pingora::{
    listeners::tls::TlsSettings,
    protocols::{http::ServerSession, l4::socket::SocketAddr, ALPN},
    server,
    services::listening::Service,
    OkOrErr, OrErr, Result,
};

use transporter::prelude::*;

// To Hide your proxy behind a real https server
// [xray] -> [transporter] -> [real-proxy]
// 1. [real-proxy] listen on socks5://:7890
// 2. [transport] listen on :8990
// 3. [xray] start with xray_client.json. listen on :1080, forward to socks5+https://127.0.0.1:8990
// 4. curl -x socks5h://127.0.0.1:1080 www.bing.com -v
//
// RUST_LOG=debug cargo run --example proxy_by_header
fn main() {
    tracing_subscriber::fmt::init();
    let opt = server::configuration::Opt::default();
    let conf = server::configuration::ServerConf {
        upstream_debug_ssl_keylog: true,
        ..Default::default()
    };
    let mut my_server = server::Server::new_with_opt_and_conf(opt, conf);
    my_server.bootstrap();

    let app = Proxy::new(ProxyByHeader::new());

    let mut srv = Service::new("proxy-by-header".to_string(), app);

    let mut tls_settings = TlsSettings::intermediate(
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/server.crt"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/server.key"),
    )
    .unwrap();
    tls_settings.set_alpn(ALPN::H2);

    srv.add_tls_with_settings("127.0.0.1:8990", None, tls_settings);

    my_server.add_service(srv);
    my_server.run_forever();
}

pub struct ProxyByHeader {
    downstream_header_keys: DownstreamHttpHeaderKeys,
}

#[async_trait::async_trait]
impl ProxyHttp for ProxyByHeader {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut ServerSession,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<MixedPeer>> {
        self.upstream_peer(session)
    }
}

pub struct DownstreamHttpHeaderKeys {
    pub peer_address: String,
    pub peer_transport_type: String,
    pub peer_sni: String,
    pub peer_skip_verify: String,
    pub peer_reuse: String,
}

impl Default for DownstreamHttpHeaderKeys {
    fn default() -> Self {
        Self {
            peer_address: "XXX-Peer-Address".to_string(),
            peer_transport_type: "XXX-Peer-Transport-Type".to_string(),
            peer_sni: "XXX-Peer-SNI".to_string(),
            peer_skip_verify: "XXX-Peer-Skip-Verify".to_string(),
            peer_reuse: "XXX-Peer-Reuse".to_string(),
        }
    }
}

impl ProxyByHeader {
    fn new() -> Self {
        Self {
            downstream_header_keys: DownstreamHttpHeaderKeys::default(),
        }
    }

    fn upstream_peer(&self, session: &mut ServerSession) -> Result<Box<MixedPeer>> {
        let transport_type = session
            .get_header(&self.downstream_header_keys.peer_transport_type)
            .or_err(
                pingora::ErrorType::InvalidHTTPHeader,
                "transport type required",
            )?
            .to_str()
            .unwrap_or_default()
            .parse::<TransportType>()
            .or_err(
                pingora::ErrorType::InvalidHTTPHeader,
                "invalid transport type",
            )?;

        let address = session
            .get_header(&self.downstream_header_keys.peer_address)
            .or_err(
                pingora::ErrorType::InvalidHTTPHeader,
                "peer address required",
            )?
            .to_str()
            .unwrap_or_default()
            .parse::<SocketAddr>()
            .or_err(
                pingora::ErrorType::InvalidHTTPHeader,
                "invalid peer address",
            )?;

        let reuse = session
            .get_header(&self.downstream_header_keys.peer_reuse)
            .is_some_and(|v| v == "true");

        match transport_type {
            TransportType::Tcp => Ok(Box::new(MixedPeer::new_tcp(address, reuse))),
            TransportType::Http => {
                let sni = session
                    .get_header(&self.downstream_header_keys.peer_sni)
                    .map(|v| v.to_str().unwrap_or_default().to_string())
                    .unwrap_or_default();
                Ok(Box::new(MixedPeer::new_http(address, sni, reuse)))
            }
            TransportType::Tls => {
                let sni = session
                    .get_header(&self.downstream_header_keys.peer_sni)
                    .map(|v| v.to_str().unwrap_or_default().to_string())
                    .unwrap_or_default();
                let skip_verify = session
                    .get_header(&self.downstream_header_keys.peer_skip_verify)
                    .is_some_and(|v| v == "true");

                let peer = MixedPeer::new_tls(address, sni, skip_verify, reuse);

                Ok(Box::new(peer))
            }
            TransportType::Https => {
                let sni = session
                    .get_header(&self.downstream_header_keys.peer_sni)
                    .map(|v| v.to_str().unwrap_or_default().to_string())
                    .unwrap_or_default();
                let skip_verify = session
                    .get_header(&self.downstream_header_keys.peer_skip_verify)
                    .is_some_and(|v| v == "true");

                let peer = MixedPeer::new_https(address, sni, skip_verify, reuse);
                Ok(Box::new(peer))
            }
        }
    }
}
