use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use tokio::sync::Mutex;

use psl::{config::AtomicPSLWorkerConfig, crypto::{CachedBlock, CryptoServiceConnector}, proto::consensus::ProtoVote, rpc::SenderType, utils::channel::{Receiver, Sender}};
use tracing::{debug, warn};

pub type VoteWithSenderAndChainId = (SenderType, u64, ProtoVote);

/// ```
///                                                          ------------------------------------
///                   Vote                      |--------->  |Block Broadcaster to Other Workers|
///                    |-------------------|    |            ------------------------------------
///                                        |    |
///                                        v    |
/// ------------------------------       ---------       -----------
/// |Block Broadcaster to Storage| ----> |Staging| ----> |LogServer|
/// ------------------------------       ---------       -----------
///                                         |            ----------------------
///                                         |----------> |Client Reply Handler|
///                                                      ----------------------
/// ```
pub struct MultiplexedStaging {
    config: AtomicPSLWorkerConfig,
    crypto: CryptoServiceConnector,

    vote_rx: Receiver<VoteWithSenderAndChainId>,
    block_rx: Receiver<CachedBlock>,

    logserver_tx: Sender<(SenderType, CachedBlock)>,
    client_reply_tx: tokio::sync::broadcast::Sender<(SenderType, u64)>,

    vote_buffer: HashMap<u64, HashMap<u64, Vec<VoteWithSenderAndChainId>>>,
    block_buffer: HashMap<u64, Vec<CachedBlock>>,

    commit_index: HashMap<u64, u64>,
    gc_tx: Sender<(SenderType, u64)>,

}

impl MultiplexedStaging {
    pub fn new(config: AtomicPSLWorkerConfig, crypto: CryptoServiceConnector, vote_rx: Receiver<VoteWithSenderAndChainId>, block_rx: Receiver<CachedBlock>, logserver_tx: Sender<(SenderType, CachedBlock)>, client_reply_tx: tokio::sync::broadcast::Sender<(SenderType, u64)>, gc_tx: Sender<(SenderType, u64)>) -> Self {
        Self {
            config,
            crypto,
            vote_rx,
            block_rx,
            logserver_tx,
            client_reply_tx,

            vote_buffer: HashMap::new(),
            block_buffer: HashMap::new(),

            commit_index: HashMap::new(),
            gc_tx,
        }
    }

    pub async fn run(staging: Arc<Mutex<Self>>) {
        let mut staging = staging.lock().await;
        staging.worker().await;
    }
    async fn worker(&mut self) {
        loop {
            let chain_id = tokio::select! {
                Some(vote) = self.vote_rx.recv() => {
                    let chain_id = vote.1;
                    self.preprocess_and_buffer_vote(vote).await;
                    chain_id
                },
                Some(block) = self.block_rx.recv() => {
                    let chain_id = block.block.chain_id;
                    self.buffer_block(block).await;
                    chain_id
                },
            };

            let new_ci = self.try_commit_blocks(chain_id);

            let old_ci = {
                let _ci = self.commit_index.entry(chain_id).or_insert(0);
                *_ci
            };

            if new_ci > old_ci {

                // Ordering here is important.
                // notify_downstream() needs to know the old commit index.
                // clean_up_buffer only works if the commit index is updated.
                self.notify_downstream(new_ci, chain_id).await;
                
                self.commit_index.insert(chain_id, new_ci);
            }
            self.clean_up_buffer(chain_id);
        }

    }

    async fn preprocess_and_buffer_vote(&mut self, vote: VoteWithSenderAndChainId) {
        let (sender, chain_id, vote) = vote;
        let vote_buffer = self.vote_buffer.entry(chain_id).or_insert(HashMap::new());
        vote_buffer
            .entry(vote.n).or_insert(Vec::new())
            .push((sender,chain_id, vote));
    }

    async fn buffer_block(&mut self, block: CachedBlock) {
        let chain_id = block.block.chain_id;
        let block_buffer = self.block_buffer.entry(chain_id).or_insert(Vec::new());
        block_buffer.push(block);
    }

    fn get_commit_threshold(&self) -> usize {
        let n = self.config.get().worker_config.storage_list.len() as usize;
        if n == 0 {
            return 0;
        }
        n / 2 + 1
    }

    fn try_commit_blocks(&mut self, chain_id: u64) -> u64 {
        let mut new_ci = *self.commit_index.get(&chain_id).unwrap_or(&0);
        let __blank = vec![];
        let __blank2 = HashMap::new();
            
        if !self.block_buffer.contains_key(&chain_id) {
            self.block_buffer.insert(chain_id, Vec::new());
        }

        let block_buffer = self.block_buffer.get(&chain_id).unwrap();

        for block in block_buffer {
            if block.block.n <= new_ci {
                continue;
            }

            let vote_buffer = self.vote_buffer.get(&chain_id).unwrap_or(&__blank2);

            let votes = vote_buffer.get(&block.block.n).unwrap_or(&__blank);
            let blk_hsh = &block.block_hash;
            let vote_set = votes.iter()
                .filter(|(_, _, vote)| blk_hsh.eq(&vote.fork_digest))
                .map(|(sender, _, _)| sender.clone())
                .collect::<HashSet<_>>();

            if vote_set.len() >= self.get_commit_threshold() {
                new_ci = block.block.n;
            } 
        }

        debug!("Try commit blocks: chain_id={}, new_ci={}", chain_id, new_ci);

        new_ci
    }

    fn clean_up_buffer(&mut self, chain_id: u64) {
        let ci = *self.commit_index.get(&chain_id).unwrap_or(&0);

        let vote_buffer = self.vote_buffer.entry(chain_id).or_insert(HashMap::new());
        vote_buffer.retain(|n, _| *n > ci);

        let block_buffer = self.block_buffer.entry(chain_id).or_insert(Vec::new());
        block_buffer.retain(|block| block.block.n > ci);
    }

    async fn notify_downstream(&mut self, new_ci: u64, chain_id: u64) {
        // Send all blocks > self.commit_index <= new_ci to the logserver.
        let me = self.config.get().net_config.name.clone();
        let me = SenderType::Auth(me, chain_id);

        let ci = *self.commit_index.get(&chain_id).unwrap_or(&0);
        let block_buffer = self.block_buffer.entry(chain_id).or_insert(Vec::new());

        for block in block_buffer {
            if block.block.n > ci && block.block.n <= new_ci {
                let _ = self.logserver_tx.send((me.clone(), block.clone())).await;
            }
        }

        if ci > 1000 {
            let _ = self.gc_tx.send((me.clone(), ci - 1000)).await;
        }


        // Send the commit index to the client reply handler.
        let _ = self.client_reply_tx.send((me.clone(), new_ci));
    }
}