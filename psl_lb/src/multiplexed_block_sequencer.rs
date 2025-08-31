use std::{pin::Pin, sync::Arc, time::Duration};

use std::collections::HashMap;
use tracing::{info, trace};
use tokio::sync::{oneshot, Mutex};

use psl::{config::AtomicPSLWorkerConfig, crypto::{default_hash, CachedBlock, CryptoServiceConnector, FutureHash}, proto::{consensus::{ProtoBlock, ProtoVectorClock, ProtoVectorClockEntry}, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase}}, rpc::SenderType, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}};

use psl::worker::cache_manager::{CacheKey, CachedValue};
use psl::worker::block_sequencer::{BlockSeqNumQuery, SequencerCommand, VectorClock};

pub struct MultiplexedBlockSequencer {
    config: AtomicPSLWorkerConfig,
    crypto: CryptoServiceConnector,

    curr_block_seq_num: HashMap<u64 /* origin id */, u64>,
    last_block_hash: HashMap<u64 /* origin id */, FutureHash>,
    self_write_op_bag: HashMap<u64 /* origin id */, Vec<(CacheKey, CachedValue)>>,
    all_write_op_bag: HashMap<u64 /* origin id */, Vec<(CacheKey, CachedValue)>>,
    curr_vector_clock: HashMap<u64 /* origin id */, VectorClock>,

    cache_manager_rx: Receiver<(SequencerCommand, u64)>,

    node_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
    storage_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,

    vc_wait_buffer: HashMap<VectorClock, Vec<oneshot::Sender<()>>>,
}

impl MultiplexedBlockSequencer {
    pub fn new(config: AtomicPSLWorkerConfig, crypto: CryptoServiceConnector,
        cache_manager_rx: Receiver<(SequencerCommand, u64)>,
        node_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
        storage_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
    ) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config, crypto,
            // curr_block_seq_num: 1,
            // last_block_hash: FutureHash::Immediate(default_hash()),
            // self_write_op_bag: Vec::new(),
            // all_write_op_bag: Vec::new(),
            // curr_vector_clock: VectorClock::new(),
            curr_block_seq_num: HashMap::new(),
            last_block_hash: HashMap::new(),
            self_write_op_bag: HashMap::new(),
            all_write_op_bag: HashMap::new(),
            curr_vector_clock: HashMap::new(),
            cache_manager_rx,
            node_broadcaster_tx,
            storage_broadcaster_tx,
            log_timer,
            vc_wait_buffer: HashMap::new(),
        }
    }

    pub async fn run(block_sequencer: Arc<Mutex<Self>>) {
        let mut block_sequencer = block_sequencer.lock().await;
        block_sequencer.log_timer.run().await;
        block_sequencer.worker().await;
    }

    async fn worker(&mut self) {
        loop {
            tokio::select! {
                command_and_chain_id = self.cache_manager_rx.recv() => {
                    let (command, chain_id) = command_and_chain_id.unwrap();
                    self.handle_command(command, chain_id).await;
                }
                _ = self.log_timer.wait() => {
                    self.log_stats().await;
                }
            }
        }
    }

    async fn log_stats(&mut self) {
        trace!("Vector Clock: {:?}", self.curr_vector_clock);
    }

    async fn handle_command(&mut self, command: SequencerCommand, chain_id: u64) {
        match command {
            SequencerCommand::SelfWriteOp { key, value, seq_num_query } => {
                self.self_write_op_bag.entry(chain_id).or_insert(Vec::new()).push((key.clone(), value.clone()));
                self.all_write_op_bag.entry(chain_id).or_insert(Vec::new()).push((key, value));

                let seq_num = self.curr_block_seq_num.entry(chain_id).or_insert(1);
                match seq_num_query {
                    BlockSeqNumQuery::DontBother => {}
                    BlockSeqNumQuery::WaitForSeqNum(sender) => {
                        sender.send(*seq_num).unwrap();
                    }
                }
            },
            SequencerCommand::OtherWriteOp { key, value } => {
                self.all_write_op_bag.entry(chain_id).or_insert(Vec::new()).push((key, value));
            },
            SequencerCommand::AdvanceVC { sender, block_seq_num } => {
                self.curr_vector_clock.entry(chain_id).or_insert(VectorClock::new()).advance(sender, block_seq_num);
                self.flush_vc_wait_buffer(chain_id).await;
            },
            SequencerCommand::MakeNewBlock => {
                self.maybe_prepare_new_block(chain_id).await;
            },
            SequencerCommand::ForceMakeNewBlock => {
                self.force_prepare_new_block(chain_id).await;
            },
            SequencerCommand::WaitForVC(vc, sender) => {
                self.buffer_vc_wait(vc, sender, chain_id).await;
            }
        }
    }

    async fn maybe_prepare_new_block(&mut self, chain_id: u64) {
        let config = self.config.get();
        let all_write_batch_size = config.worker_config.all_writes_max_batch_size;
        let self_write_batch_size = config.worker_config.self_writes_max_batch_size;

        if self.all_write_op_bag.get(&chain_id).unwrap_or(&Vec::new()).len() < all_write_batch_size  || self.self_write_op_bag.get(&chain_id).unwrap_or(&Vec::new()).len() < self_write_batch_size {
            return; // Not enough writes to form a block
        }

        self.do_prepare_new_block(chain_id).await;
    }

    async fn force_prepare_new_block(&mut self, chain_id: u64) {
        if self.all_write_op_bag.is_empty() {
            return;
        }
        
        self.do_prepare_new_block(chain_id).await;
    }

    async fn do_prepare_new_block(&mut self, chain_id: u64) {
        let _seq_num = self.curr_block_seq_num.entry(chain_id).or_insert(1);
        let seq_num = *_seq_num;
        *_seq_num += 1;


        let origin = self.config.get().net_config.name.clone();
        let me = SenderType::Auth(origin.clone(), 0);

        let __default_vc = VectorClock::new();
        let _read_vc = self.curr_vector_clock.entry(chain_id).or_insert(__default_vc);
        let read_vc = _read_vc.clone();
        _read_vc.advance(me.clone(), seq_num);
        assert!(read_vc.get(&me) + 1 == seq_num);

        let all_writes = Self::wrap_vec(
            Self::dedup_vec(self.all_write_op_bag.entry(chain_id).or_insert(Vec::new()).drain(..)),
            seq_num,
            Some(_read_vc.serialize()),
            origin.clone(),
            chain_id,
        );

        let self_writes = Self::wrap_vec(
            Self::dedup_vec(self.self_write_op_bag.entry(chain_id).or_insert(Vec::new()).drain(..)),
            seq_num,
            Some(read_vc.serialize()),
            origin,
            chain_id,
        );


        let (all_writes_rx, _, _) = self.crypto.prepare_block(
            all_writes,
            false,
            FutureHash::Immediate(default_hash())
        ).await;

        let _last_block_hash = self.last_block_hash.entry(chain_id).or_insert(FutureHash::Immediate(default_hash()));
        let parent_hash_rx = _last_block_hash.take();
        let (self_writes_rx, hash_rx, hash_rx2) = self.crypto.prepare_block(
            self_writes,
            true,
            parent_hash_rx,
        ).await;
        *_last_block_hash = FutureHash::Future(hash_rx);

        // Nodes get all writes so as to virally send writes from other nodes.
        self.node_broadcaster_tx.send(all_writes_rx).await;

        // Storage only gets self writes.
        // Strong convergence will ensure that the checkpoint state matches the state using all_writes above.
        // Same VC => Same state.
        self.storage_broadcaster_tx.send(self_writes_rx).await;

        // TODO: Send hash_rx2 to client reply handler.
    }

    fn wrap_vec(
        writes: Vec<(CacheKey, CachedValue)>,
        seq_num: u64,
        vector_clock: Option<ProtoVectorClock>,
        origin: String,
        chain_id: u64,
    ) -> ProtoBlock {
        ProtoBlock {
            tx_list: writes.into_iter()
                .map(|(key, value)| ProtoTransaction {
                    on_receive: None,
                    on_crash_commit: Some(ProtoTransactionPhase {
                        ops: vec![ProtoTransactionOp { 
                            op_type: ProtoTransactionOpType::Write as i32,
                            operands: vec![key, bincode::serialize(&value).unwrap()], 
                        }],
                    }),
                    on_byzantine_commit: None,
                    is_reconfiguration: false,
                    is_2pc: false,
                })
                .collect(),
            n: seq_num,
            parent: vec![],
            view: 0,
            qc: vec![],
            fork_validation: vec![],
            view_is_stable: true,
            config_num: 1,
            sig: None,
            vector_clock,
            origin,
            chain_id, // This field is generally unused.
        }
    }

    fn dedup_vec(vec: std::vec::Drain<(CacheKey, CachedValue)>) -> Vec<(CacheKey, CachedValue)> {
        let mut seen = HashMap::new();

        for (key, value) in vec {
            let entry = seen.entry(key).or_insert(value.clone());

            entry.merge_cached(value);
        }

        seen.into_iter().collect()
    }


    async fn flush_vc_wait_buffer(&mut self, chain_id: u64) {
        let mut to_remove = Vec::new();

        for (vc, _) in self.vc_wait_buffer.iter() {
            if self.curr_vector_clock.get(&chain_id).unwrap_or(&VectorClock::new()) >= vc {
                to_remove.push(vc.clone());
            }
        }

        for vc in to_remove.iter() {
            let mut senders = self.vc_wait_buffer.remove(vc).unwrap();
            for sender in senders.drain(..) {
                let _ = sender.send(());
            }
        }
    }

    async fn buffer_vc_wait(&mut self, vc: VectorClock, sender: oneshot::Sender<()>, chain_id: u64) {
        let buffer = self.vc_wait_buffer.entry(vc).or_insert(Vec::new());
        buffer.push(sender);

        self.flush_vc_wait_buffer(chain_id).await;
    }
}
