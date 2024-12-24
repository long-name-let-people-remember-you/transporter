use std::sync::Arc;

use bytes::Bytes;
use futures_util::FutureExt;
use http::header;
use log::{debug, error, trace, warn};
use peer::{MixedPeer, TransportType};
use pingora::{
    apps::{HttpServerApp, HttpServerOptions},
    connectors::{self},
    http::ResponseHeader,
    protocols::{
        http::{v2::server, HttpTask, ServerSession},
        Stream,
    },
    server::ShutdownWatch,
    Error,
    ErrorType::InternalError,
    OrErr, Result,
};
use proxy_trait::ProxyHttp;
use tokio::{
    sync::{mpsc, Notify},
    time,
};

pub mod peer;
pub mod proxy_h2;
pub mod proxy_tcp;
pub mod proxy_trait;

const TASK_BUFFER_SIZE: usize = 4;
const UPSTREAM_READ_BUFFER_SIZE: usize = 1024 * 16;

pub struct Proxy<PH> {
    pub inner: PH,
    pub transport_connector: connectors::TransportConnector,
    pub h2_connector: connectors::http::v2::Connector,
    shutdown: Notify,
    pub server_options: Option<HttpServerOptions>,
    pub retry_times: usize,
}

impl<PH: ProxyHttp> Proxy<PH>
where
    PH: ProxyHttp + Send + Sync,
    PH::CTX: Send + Sync,
{
    pub fn new(inner: PH) -> Self {
        let opt = HttpServerOptions::default();
        Self {
            inner,
            transport_connector: connectors::TransportConnector::new(None),
            h2_connector: connectors::http::v2::Connector::new(None),
            shutdown: Notify::new(),
            server_options: Some(opt),
            retry_times: 3,
        }
    }

    async fn process_request(
        self: &Arc<Self>,
        mut session: ServerSession,
        mut ctx: PH::CTX,
    ) -> Option<Stream> {
        match self.inner.request_filter(&mut session, &mut ctx).await {
            Ok(response_sent) => {
                if response_sent {
                    return self.finish(session, &mut ctx, true, None).await;
                }
                /* else continue */
            }
            Err(e) => {
                self.handle_error(&mut session, &mut ctx, e, "Fail to filter request:")
                    .await;
                return None;
            }
        }
        let mut retries = 0;
        let mut server_reuse: bool = false;
        let mut proxy_error: Option<Box<Error>> = None;

        while retries < self.retry_times {
            retries += 1;

            let (reuse, e) = self.proxy_to_upstream(&mut session, &mut ctx).await;
            server_reuse = reuse;
            debug!("reuse: {reuse:?} error: {e:?}");
            match e {
                Some(error) => {
                    let retry = error.retry();
                    proxy_error = Some(error);
                    if !retry {
                        break;
                    }

                    // only log error that will be retried here, the final error will be logged bellow
                    warn!(
                        "Fail to proxy: {}, tries: {}, retry: {}, {}",
                        proxy_error.as_ref().unwrap(),
                        retries,
                        retry,
                        self.inner.request_summary(&session, &ctx)
                    )
                }
                None => {
                    proxy_error = None;
                    break;
                }
            }
        }

        let final_error = {
            if let Some(e) = proxy_error.as_ref() {
                let status = self.inner.fail_to_proxy(&mut session, e, &mut ctx).await;
                if !self.inner.suppress_error_log(&mut session, &mut ctx, e) {
                    // remote cancel/close stream is not an error?
                    // if !e.root_cause().downcast_ref::<h2::Error>().is_some_and(|e| {
                    //     e.reason()
                    //         .is_some_and(|r| r == h2::Reason::CANCEL || r == h2::Reason::NO_ERROR)
                    // }) {
                    // }
                    error!(
                        "Fail to proxy: {}, status: {}, tries: {}, retry: {}, {}",
                        &e,
                        status,
                        retries,
                        retries,
                        self.inner.request_summary(&session, &ctx)
                    );
                }
                Some(e.as_ref())
            } else {
                None
            }
        };
        self.finish(session, &mut ctx, server_reuse, final_error)
            .await
    }

    // return bool: server_session can be reused, and error if any
    async fn proxy_to_upstream(
        &self,
        session: &mut ServerSession,
        ctx: &mut PH::CTX,
    ) -> (bool, Option<Box<Error>>) {
        let peer = match self.inner.upstream_peer(session, ctx).await {
            Ok(p) => p,
            Err(e) => return (false, Some(e)),
        };

        match peer.transport_type {
            TransportType::Tcp | TransportType::Tls => self.proxy_to_tcp(session, ctx, &peer).await,
            TransportType::Http | TransportType::Https => {
                // H2 only, cause h1 proxy is not standardized.
                // The standard h1 needs to have Content-Length or use chunks?
                self.proxy_to_h2(session, ctx, &peer).await
            } // TODO: websocket? handle h1?
        }
    }

    async fn proxy_handle_downstream(
        &self,
        session: &mut ServerSession,
        tx: mpsc::Sender<HttpTask>,
        mut rx: mpsc::Receiver<HttpTask>,
        ctx: &mut PH::CTX,
    ) -> Result<bool> {
        let buffer = session.get_retry_buffer();
        let mut request_done = session.is_body_done();

        // send buffer if it exists or body empty(do inner.request_body_filter)
        if buffer.is_some() || session.is_body_empty() {
            let sender_permit = tx
                .reserve()
                .await
                .or_err(InternalError, "reserving body pipe")?;
            self.send_body_to_pipe(session, buffer, request_done, sender_permit, ctx)
                .await?;
        }

        let mut response_done = false;
        while !request_done || !response_done {
            let send_permit = tx
                .try_reserve()
                .or_err(InternalError, "try_reserve() body pipe for upstream");

            tokio::select! {
                // only try to send to pipe if there is capacity to avoid deadlock
                body = session.read_body_or_idle(request_done), if !request_done && send_permit.is_ok() => {
                    debug!("downstream event {:?}", body);

                    let body = match body {
                        Ok(b) => b,
                        Err(e) => {
                            return Err(e.into_down());
                        }
                    };
                    // , if downstream_state.can_poll() && send_permit.is_ok() =>
                    // If the request is websocket, `None` body means the request is closed.
                    // Set the response to be done as well so that the request completes normally.
                    if body.is_none() && session.is_upgrade_req() {
                        response_done = true;
                    }

                    let is_body_done = session.is_body_done();
                    let req_done = self.send_body_to_pipe(session, body, is_body_done, send_permit.unwrap(), ctx).await?;
                    request_done |= req_done;
                },
                _ = tx.reserve(), if !request_done && send_permit.is_err() => {
                    debug!("waiting for permit {send_permit:?}");
                },
                task = rx.recv(), if !response_done => {
                    debug!("upstream event: {:?}", task);
                    if let Some(t) = task {
                        let mut tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
                        tasks.push(t);

                        // pull as many tasks as we can
                        while let Some(maybe_task) = rx.recv().now_or_never() {
                            debug!("upstream event now: {:?}", maybe_task);
                            if let Some(t) = maybe_task {
                                tasks.push(t);
                            } else {
                                break;
                            }
                        }

                        let mut filtered_tasks = Vec::with_capacity(tasks.len());
                        for t in tasks {
                            let task = self.response_filter(session, t, ctx).await?;
                            filtered_tasks.push(task);
                        }
                        if session.response_duplex_vec(filtered_tasks).await? {
                            response_done = true;
                        }
                        request_done |= session.is_body_done();
                    } else {
                        debug!("empty upstream event");
                        response_done = true;
                    }
                }
                else => {
                    break;
                }
            }
        }

        let mut reuse_downstream = true;
        match session.finish_body().await {
            Ok(_) => {
                debug!("finished sending body to downstream")
            }
            Err(e) => {
                error!("Error finish sending body to downstream: {}", e);
                reuse_downstream = false;
            }
        }
        Ok(reuse_downstream)
    }

    async fn send_body_to_pipe(
        &self,
        session: &mut ServerSession,
        mut data: Option<Bytes>,
        end_of_body: bool,
        tx: mpsc::Permit<'_, HttpTask>,
        ctx: &mut PH::CTX,
    ) -> Result<bool> {
        // Note: end of body
        // this var is to signal if downstream finish sending the body, which shouldn't be
        // affected by the request body filter
        let end_of_body = end_of_body || data.is_none();

        self.inner
            .request_body_filter(session, &mut data, end_of_body, ctx)
            .await?;

        // the flag to signal to upstream
        let upstream_end_of_body = end_of_body || data.is_none();

        /* It is normal to get 0 bytes because of multi-chunk or request_body_filter decides not to
         * output anything yet.
         * Don't write 0 bytes to the network since it will be
         * treated as terminating chunk */
        if !upstream_end_of_body && data.as_ref().map_or(false, |d| d.is_empty()) {
            return Ok(false);
        }

        debug!(
            "Read {} bytes body from downstream",
            data.as_ref().map_or(-1, |d| d.len() as isize)
        );
        tx.send(HttpTask::Body(data, upstream_end_of_body));

        Ok(end_of_body)
    }

    async fn response_filter(
        &self,
        session: &mut ServerSession,
        task: HttpTask,
        ctx: &mut PH::CTX,
    ) -> Result<HttpTask> {
        match task {
            HttpTask::Header(mut header, eos) => {
                self.inner
                    .upstream_response_filter(session, &mut header, ctx);
                // TODO: insert headers
                match self.inner.response_filter(session, &mut header, ctx).await {
                    Ok(_) => Ok(HttpTask::Header(header, eos)),
                    Err(e) => Err(e),
                }
            }
            HttpTask::Body(mut data, eos) => {
                self.inner
                    .upstream_response_body_filter(session, &mut data, eos, ctx)?;

                if let Some(duration) = self
                    .inner
                    .response_body_filter(session, &mut data, eos, ctx)?
                {
                    trace!("delaying response for {:?}", duration);
                    time::sleep(duration).await;
                }
                Ok(HttpTask::Body(data, eos))
            }
            _ => Ok(task),
        }
    }

    async fn handle_error(
        &self,
        session: &mut ServerSession,
        ctx: &mut PH::CTX,
        e: Box<Error>,
        context: &str,
    ) {
        if !self.inner.suppress_error_log(session, ctx, &e) {
            error!(
                "{context} {}, {}",
                e,
                self.inner.request_summary(session, ctx)
            );
        }
        self.inner.fail_to_proxy(session, &e, ctx).await;
        self.inner.logging(session, Some(&e), ctx).await;
    }

    async fn finish(
        &self,
        mut session: ServerSession,
        ctx: &mut PH::CTX,
        reuse: bool,
        error: Option<&Error>,
    ) -> Option<Stream> {
        self.inner.logging(&mut session, error, ctx).await;
        if reuse {
            session.finish().await.ok().flatten()
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl<PH> HttpServerApp for Proxy<PH>
where
    PH: ProxyHttp + Send + Sync + 'static,
    PH::CTX: Send + Sync,
{
    /// Similar to the [`ServerApp`], this function is called whenever a new HTTP session is established.
    ///
    /// After successful processing, [`ServerSession::finish()`] can be called to return an optionally reusable
    /// connection back to the service. The caller needs to make sure that the connection is in a reusable state
    /// i.e., no error or incomplete read or write headers or bodies. Otherwise a `None` should be returned.
    async fn process_new_http(
        self: &Arc<Self>,
        mut session: ServerSession,
        // TODO: make this ShutdownWatch so that all task can await on this event
        shutdown: &ShutdownWatch,
    ) -> Option<Stream> {
        let read_request = tokio::select! {
            biased; // biased select is cheaper, and we don't want to drop already buffered request
            res = session.read_request() => { res }
            _ = self.shutdown.notified() => {
                // service shutting down, dropping the connection to stop more request from coming in
                return None;
            }
        };

        match read_request {
            Ok(true) => {
                debug!("Successfully get a new request")
            }
            Ok(false) => return None,
            Err(mut e) => {
                e.as_down();
                error!("Fail to proxy: {}", e);
                return None;
            }
        }
        if *shutdown.borrow() {
            session.set_keepalive(None);
        } else {
            session.set_keepalive(Some(60));
        }
        let ctx = self.inner.new_ctx();
        self.process_request(session, ctx).await
    }

    /// Provide options on how HTTP/2 connection should be established. This function will be called
    /// every time a new HTTP/2 **connection** needs to be established.
    ///
    /// A `None` means to use the built-in default options. See [`server::H2Options`] for more details.
    fn h2_options(&self) -> Option<server::H2Options> {
        self.inner.h2_options()
    }

    /// Provide HTTP server options used to override default behavior. This function will be called
    /// every time a new connection is processed.
    ///
    /// A `None` means no server options will be applied.
    fn server_options(&self) -> Option<&HttpServerOptions> {
        self.server_options.as_ref()
    }

    async fn http_cleanup(&self) {
        // Notify all keepalived requests blocking on read_request() to abort
        self.shutdown.notify_waiters();

        // TODO: impl shutting down flag so that we don't need to read stack.is_shutting_down()
    }
}

pub(crate) fn generate_ok_response() -> ResponseHeader {
    let mut resp_header = ResponseHeader::build(200, Some(1)).unwrap();
    resp_header
        .insert_header(header::CACHE_CONTROL, "private, no-store")
        .unwrap();
    resp_header
}
