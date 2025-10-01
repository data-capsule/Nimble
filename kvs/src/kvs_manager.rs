use psl::{config::AtomicConfig, consensus::batch_proposal::MsgAckChanWithTag, crypto::HashType, proto::execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType}, utils::channel::{make_channel, Receiver, Sender}, worker::TxWithAckChanTag};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, info, trace, warn};
use std::sync::Arc;
use eth_trie::{MemoryDB, EthTrie, Trie};


pub struct KVSManager {
    config: AtomicConfig,
    store: EthTrie<MemoryDB>,
    batch_proposal_rx: Receiver<TxWithAckChanTag>,
    reply_tx: Sender<(Vec<(ProtoTransactionOpResult, Option<Receiver<()>>)>, MsgAckChanWithTag)>,
    nimble_tx: Sender<(Sender<()>, Vec<u8>)>,
}

impl KVSManager {
    pub fn new(
        config: AtomicConfig,
        batch_proposal_rx: Receiver<TxWithAckChanTag>,
        reply_tx: Sender<(Vec<(ProtoTransactionOpResult, Option<Receiver<()>>)>, MsgAckChanWithTag)>,
        nimble_tx: Sender<(Sender<()>, HashType)>,
    ) -> Self {

        let store = EthTrie::new(Arc::new(MemoryDB::new(false)));
        Self {
            config,
            store,
            batch_proposal_rx,
            reply_tx,
            nimble_tx,
        }
    }

    pub async fn run(manager: Arc<Mutex<Self>>) {
        debug!("KVSManager started");
        // TODO: Implement actual manager logic
        let mut manager = manager.lock().await;
        loop {
            let resp = manager.worker().await;
            debug!("KVSManager worker: received response: {:?}", resp);
        }
    }

    async fn worker(&mut self) -> Option<()> {
        let (tx, ack_chan) = self.batch_proposal_rx.recv().await?;

        let tx = tx?;

        // let crash_commit = tx.on_crash_commit?;
        let receive = tx.on_receive?;

        let ops = &receive.ops;

        let mut results = Vec::new();
        for op in ops {
            results.push(self.process_op(op).await);
        }

        self.reply_tx.send((results, ack_chan)).await;

        Some(())

    }

    async fn process_op(&mut self, op: &ProtoTransactionOp) -> (ProtoTransactionOpResult, Option<Receiver<()>>) {
        match op.op_type() {
            ProtoTransactionOpType::Write => {
                let (_tx, _rx) = make_channel(1);
                // let _ = self.store.insert(&op.operands[0], &op.operands[1])
                // let root_hash = self.store.root_hash().unwrap().to_vec();
                let hsh = op.operands[0].clone();
                let _resp = self.nimble_tx.send((_tx, hsh)).await;
                
                (ProtoTransactionOpResult { success: true, values: vec![] }, Some(_rx))
            },
            // ProtoTransactionOpType::Read => {
            //     let Ok(Some(value)) = self.store.get(&op.operands[0]) else {
            //         trace!("KVSManager worker: read operation failed: {:?}", op.operands[0]);
            //         return (ProtoTransactionOpResult { success: false, values: vec![] }, None);
            //     };
            //     (ProtoTransactionOpResult { success: true, values: vec![value] }, None)
            // },
            _ => {
                warn!("Unsupported operation: {:?}", op.op_type());
                (ProtoTransactionOpResult { success: false, values: vec![] }, None)
            }
        }
    }

}
