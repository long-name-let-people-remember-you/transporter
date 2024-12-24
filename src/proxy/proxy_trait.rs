use std::time::Duration;

use bytes::Bytes;
use pingora::{
    http::{RequestHeader, ResponseHeader},
    protocols::Digest,
    ErrorSource,
    ErrorType::{ConnectionClosed, HTTPStatus, ReadError, WriteError},
};

use super::*;

/// The interface to control Proxy
#[async_trait::async_trait]
pub trait ProxyHttp {
    /// The per request object to share state across the different filters
    type CTX;

    /// Define how the `ctx` should be created.
    fn new_ctx(&self) -> Self::CTX;

    /// Handle the incoming request.
    ///
    /// In this phase, users can parse, validate, rate limit, perform access control and/or
    /// return a response for this request.
    ///
    /// If the user already sent a response to this request, an `Ok(true)` should be returned so that
    /// the proxy would exit. The proxy continues to the next phases when `Ok(false)` is returned.
    ///
    /// By default this filter does nothing and returns `Ok(false)`.
    async fn request_filter(
        &self,
        _session: &mut ServerSession,
        _ctx: &mut Self::CTX,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Define where the proxy should send the request to.
    ///
    /// The returned [MixedPeer] contains the information regarding where and how this request should
    /// be forwarded to.
    async fn upstream_peer(
        &self,
        session: &mut ServerSession,
        ctx: &mut Self::CTX,
    ) -> Result<Box<MixedPeer>>;

    /// Handle the incoming request body.
    ///
    /// This function will be called every time a piece of request body is received. The `body` is
    /// **not the entire request body**
    ///
    /// The async nature of this function allows to throttle the upload speed and/or executing
    /// heavy computation logic such as WAF rules on offloaded threads without blocking th threads
    /// who process the request themselves.
    async fn request_body_filter(
        &self,
        _session: &mut ServerSession,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    /// Modify the request before it is sent to the upstream
    ///
    /// Unlike [Self::request_filter()], this filter allows to change the request headers to send
    /// to the upstream.
    /// Only works if upstream is http/https
    async fn upstream_request_filter(
        &self,
        _session: &mut ServerSession,
        _upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    /// Modify the response header from the upstream
    fn upstream_response_filter(
        &self,
        _session: &mut ServerSession,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) {
    }

    /// Modify the response header before it is send to the downstream
    async fn response_filter(
        &self,
        _session: &mut ServerSession,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    /// Similar to [Self::upstream_response_filter()] but for response body
    ///
    /// This function will be called every time a piece of response body is received. This `body` is
    /// **not the entire response body**
    fn upstream_response_body_filter(
        &self,
        _session: &mut ServerSession,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    /// Similar to [Self::response_filter()] but for response body chunks
    fn response_body_filter(
        &self,
        _session: &mut ServerSession,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        Ok(None)
    }

    async fn logging(
        &self,
        _session: &mut ServerSession,
        _e: Option<&Error>,
        _ctx: &mut Self::CTX,
    ) {
    }

    /// This callback is invoked every time request related error log needs to be generated
    ///
    /// Users can define what is important to be written about this request via the returned string.
    fn request_summary(&self, session: &ServerSession, _ctx: &Self::CTX) -> String {
        session.request_summary()
    }

    /// A value of true means that the log message will be suppressed. The default value is false.
    fn suppress_error_log(
        &self,
        _session: &mut ServerSession,
        _ctx: &mut Self::CTX,
        _error: &Error,
    ) -> bool {
        false
    }

    /// This filter is called when the request encounters a fatal error.
    ///
    /// Users may write an error response to the downstream if the downstream is still writeable.
    ///
    /// The response status code of the error response maybe returned for logging purpose.
    async fn fail_to_proxy(
        &self,
        session: &mut ServerSession,
        e: &Error,
        _ctx: &mut Self::CTX,
    ) -> u16 {
        let code = match e.etype() {
            HTTPStatus(code) => *code,
            _ => {
                match e.esource() {
                    ErrorSource::Upstream => 502,
                    ErrorSource::Downstream => {
                        match e.etype() {
                            WriteError | ReadError | ConnectionClosed => {
                                /* connection already dead */
                                0
                            }
                            _ => 400,
                        }
                    }
                    ErrorSource::Internal | ErrorSource::Unset => 500,
                }
            }
        };

        if code > 0 {
            session.respond_error(code).await;
        }

        code
    }

    /// This filter is called when the request just established or reused a connection to the upstream
    ///
    /// This filter allows user to log timing and connection related info.
    async fn connect_to_upstream(
        &self,
        _session: &mut ServerSession,
        _reused: bool,
        _peer: &MixedPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _dist: Option<&Digest>,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    fn h2_options(&self) -> Option<server::H2Options> {
        None
    }
}
