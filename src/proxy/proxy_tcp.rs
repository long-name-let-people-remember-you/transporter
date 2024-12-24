use std::time::Duration;

use pingora::{
    prelude::timeout,
    protocols::Digest,
    upstreams::peer::Peer,
    ErrorType::{ReadError, ReadTimedout, WriteError, WriteTimedout},
    RetryType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

impl<PH> Proxy<PH>
where
    PH: ProxyHttp + Send + Sync,
    PH::CTX: Send + Sync,
{
    pub(crate) async fn proxy_to_tcp(
        &self,
        session: &mut ServerSession,
        ctx: &mut PH::CTX,
        peer: &MixedPeer,
    ) -> (bool, Option<Box<Error>>) {
        match self.connect(peer).await {
            Ok((mut stream, upstream_reused)) => {
                #[cfg(windows)]
                let raw = stream.id() as std::os::windows::io::RawSocket;
                #[cfg(unix)]
                let raw = stream.id();

                let dig = Digest {
                    ssl_digest: stream.get_ssl_digest(),
                    timing_digest: stream.get_timing_digest(),
                    proxy_digest: stream.get_proxy_digest(),
                    socket_digest: stream.get_socket_digest(),
                };
                if let Err(e) = self
                    .inner
                    .connect_to_upstream(session, upstream_reused, peer, raw, Some(&dig), ctx)
                    .await
                {
                    return (false, Some(e));
                }

                let (tx_upstream, rx_upstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);
                let (tx_downstream, rx_downstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);

                session.enable_retry_buffering();

                let ret = tokio::try_join!(
                    self.proxy_handle_downstream(session, tx_downstream, rx_upstream, ctx),
                    self.proxy_handle_tcp_upstream(&mut stream, tx_upstream, rx_downstream, peer)
                );

                match ret {
                    Ok((downstream_can_reuse, _)) => {
                        if peer.reuse {
                            self.transport_connector.release_stream(
                                stream,
                                peer.reuse_hash(),
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

    pub(crate) async fn proxy_handle_tcp_upstream(
        &self,
        stream: &mut Stream,
        tx: mpsc::Sender<HttpTask>,
        mut rx: mpsc::Receiver<HttpTask>,
        peer: &MixedPeer,
    ) -> Result<()> {
        tx.send(HttpTask::Header(Box::new(generate_ok_response()), false))
            .await
            .or_err(InternalError, "Failed to send 'header' to pipe")?;

        let mut request_done = false;
        let mut response_done = false;
        let mut buf = [0; UPSTREAM_READ_BUFFER_SIZE];

        while !request_done || !response_done {
            tokio::select! {
                res = self.stream_read_bytes(stream, &mut buf, peer.options.read_timeout), if !response_done => {
                    match res {
                        Ok(Some(data)) => {
                            tx.send(HttpTask::Body(Some(data), false)).await.or_err(InternalError, "Failed to send 'body' to pipe")?;
                        }
                        Ok(None) => {
                            response_done = true;
                            tx.send(HttpTask::Done).await.or_err(InternalError, "Failed to send 'done' to pipe")?;
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
                                self.stream_write_bytes(stream, &data, peer).await?;
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

    pub(crate) async fn connect(&self, peer: &MixedPeer) -> Result<(Stream, bool)> {
        if peer.reuse {
            self.transport_connector.get_stream(peer).await
        } else {
            let peer = self.transport_connector.new_stream(peer).await?;
            Ok((peer, false))
        }
    }

    pub(crate) async fn stream_read_bytes(
        &self,
        stream: &mut Stream,
        buf: &mut [u8],
        read_timeout: Option<Duration>,
    ) -> Result<Option<Bytes>> {
        let read_result = match read_timeout {
            Some(t) => timeout(t, stream.read(buf))
                .await
                .or_err(ReadTimedout, "while stream reading")?,
            None => stream.read(buf).await,
        };

        match read_result {
            Ok(0) => Ok(None),
            Ok(n) => Ok(Some(Bytes::copy_from_slice(&buf[..n]))),
            Err(e) => {
                let true_io_error = e.raw_os_error().is_some();
                let mut e = Error::because(ReadError, "while stream reading", e);
                if true_io_error {
                    e.retry = RetryType::ReusedOnly;
                }
                Err(e)
            }
        }
    }

    pub(crate) async fn stream_write_bytes(
        &self,
        stream: &mut Stream,
        data: &[u8],
        peer: &MixedPeer,
    ) -> Result<()> {
        match peer.options.write_timeout {
            Some(t) => timeout(t, stream.write_all(data))
                .await
                .or_err(WriteTimedout, "while stream writing")?
                .map_err(|e| Error::because(WriteError, "while stream writing", e))?,
            None => stream
                .write_all(data)
                .await
                .map_err(|e| Error::because(WriteError, "while stream writing", e))?,
        };

        stream
            .flush()
            .await
            .map_err(|e| Error::because(WriteError, "flushing body", e))
    }
}
