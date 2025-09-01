use psl::{config::AtomicConfig, consensus::batch_proposal::MsgAckChanWithTag, proto::{client::{proto_client_reply::Reply, ProtoClientReply, ProtoTransactionReceipt}, execution::{ProtoTransactionOpResult, ProtoTransactionResult}}, rpc::{server::LatencyProfile, PinnedMessage}, utils::channel::Receiver};
use tokio::sync::{oneshot, Mutex};
use tracing::debug;
use std::sync::Arc;
use prost::Message as _;

pub struct ClientReplyHandler {
    config: AtomicConfig,
    reply_rx: Receiver<(Vec<(ProtoTransactionOpResult, Option<Receiver<()>>)>, MsgAckChanWithTag)>,
}

impl ClientReplyHandler {
    pub fn new(config: AtomicConfig, reply_rx: Receiver<(Vec<(ProtoTransactionOpResult, Option<Receiver<()>>)>, MsgAckChanWithTag)>) -> Self {
        Self { config, reply_rx }
    }

    pub async fn run(handler: Arc<Mutex<Self>>) -> Option<()> {
        debug!("ClientReplyHandler started");
        let mut handler = handler.lock().await;
        loop {
            let (results, (ack_chan, client_tag, _sender)) = handler.reply_rx.recv().await?;
            let mut _results = Vec::new();
            
            
            for (result, rx) in results {
                if let Some(rx) = rx {
                    let _ = rx.recv().await;
                }

                _results.push(result);
            }

            let reply = ProtoClientReply {
                client_tag,
                reply: Some(Reply::Receipt(ProtoTransactionReceipt {
                    req_digest: vec![],
                    block_n: 0,
                    tx_n: 0,
                    results: Some(ProtoTransactionResult {
                        result: _results,
                    }),
                    await_byz_response: false,
                    byz_responses: vec![],
                })),
            };

            let buf = reply.encode_to_vec();
            let sz = buf.len();
            let msg = PinnedMessage::from(buf, sz, psl::rpc::SenderType::Anon);

            let profile = LatencyProfile::new();
            
            let _ = ack_chan.send((msg.clone(), profile)).await;
            
        }
    }
}
