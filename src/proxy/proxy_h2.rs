use pingora::{
    protocols::http::{client::HttpSession, v2::client::Http2Session},
    upstreams::peer::Peer,
};

use super::*;

impl<PH> Proxy<PH>
where
    PH: ProxyHttp + Send + Sync,
    PH::CTX: Send + Sync,
{
    pub(crate) async fn proxy_to_h2(
        &self,
        session: &mut ServerSession,
        ctx: &mut PH::CTX,
        peer: &MixedPeer,
    ) -> (bool, Option<Box<Error>>) {
        match self.connect_h2(peer).await {
            Ok((mut client_session, upstream_reused)) => {
                #[cfg(unix)]
                let raw = client_session.fd();
                #[cfg(windows)]
                let raw = client_session.fd() as std::os::windows::io::RawSocket;

                if let Err(e) = self
                    .inner
                    .connect_to_upstream(
                        session,
                        upstream_reused,
                        peer,
                        raw,
                        client_session.digest(),
                        ctx,
                    )
                    .await
                {
                    return (false, Some(e));
                }

                let mut req = session.req_header().clone();
                if let Err(e) = self
                    .inner
                    .upstream_request_filter(session, &mut req, ctx)
                    .await
                {
                    return (false, Some(e));
                }

                if let Err(e) = client_session.write_request_header(Box::new(req), false) {
                    return (false, Some(e));
                }

                let (tx_upstream, rx_upstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);
                let (tx_downstream, rx_downstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);

                let ret = tokio::try_join!(
                    self.proxy_handle_downstream(session, tx_downstream, rx_upstream, ctx),
                    self.proxy_handle_h2_upstream(&mut client_session, tx_upstream, rx_downstream)
                );

                match ret {
                    Ok((downstream_can_reuse, _)) => {
                        if peer.reuse {
                            self.h2_connector.release_http_session(
                                client_session,
                                peer,
                                peer.idle_timeout(),
                            );
                        }
                        (downstream_can_reuse, None)
                    }
                    Err(e) => (false, Some(e)),
                }
            }
            Err(e) => (false, Some(e)),
        }
    }

    async fn proxy_handle_h2_upstream(
        &self,
        client_session: &mut Http2Session,
        tx: mpsc::Sender<HttpTask>,
        mut rx: mpsc::Receiver<HttpTask>,
    ) -> Result<()> {
        let mut request_done = false;
        let mut response_done = false;

        client_session
            .read_response_header()
            .await
            .map_err(|e| e.into_up())?;

        let resp_header = Box::new(client_session.response_header().expect("just read").clone());

        tx.send(HttpTask::Header(
            resp_header,
            client_session.response_finished(),
        ))
        .await
        .or_err(InternalError, "sending h2 headers to pipe")?;

        while !request_done || !response_done {
            tokio::select! {
                res = client_session.read_response_body() => {
                    match res {
                        Ok(Some(data)) => {
                            tx.send(HttpTask::Body(Some(data), false)).await.or_err(InternalError, "Failed to send h2 body to pipe")?;
                        }
                        Ok(None) => {
                            response_done = true;
                            tx.send(HttpTask::Done).await.or_err(InternalError, "Failed to send h2 done to pipe")?;
                        }
                        Err(e) => {
                            let _ = tx.send(HttpTask::Failed(e.into_down())).await;
                            return Ok(());
                        }
                    };
                }
                body = rx.recv(), if !request_done => {
                    match body {
                        Some(HttpTask::Body(data, end)) => {
                            request_done = end;
                            if let Some(data) = data {
                                client_session.write_request_body(data, request_done)?;
                            }
                        }
                        _ => {
                            warn!("Unexpected task sent to upstream");
                            request_done = true;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn connect_h2(&self, peer: &MixedPeer) -> Result<(Http2Session, bool)> {
        if peer.reuse {
            if let Ok(Some(client_session)) = self.h2_connector.reused_http_session(peer).await {
                return Ok((client_session, true));
            }
        }
        match self.h2_connector.new_http_session(peer).await? {
            HttpSession::H2(h2) => Ok((h2, false)),
            _ => Error::e_explain(pingora::ErrorType::H2Downgrade, "upstream downgrade to h1"),
        }
    }
}
