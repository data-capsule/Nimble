use std::{collections::HashMap, sync::{atomic::{AtomicU64, Ordering}, RwLock}, time::Instant};

use async_trait::async_trait;
use ledger::{Block, CustomSerde, Handle, NimbleDigest, Nonce, Nonces, Receipt, Receipts};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use std::sync::Arc;

use crate::{errors::{LedgerStoreError, StorageError}, ledger::{LedgerEntry, LedgerStore}, psl_proto::{ReadRemoteReq, StoreRemoteReq}};

use crate::psl_proto::psl_storage_call_client::PslStorageCallClient;

pub struct PSLStorageConnector {
    connection: Arc<RwLock<PslStorageCallClient<Channel>>>,
    view_handle: Handle,
    tail_index: Arc<Mutex<HashMap<Handle, usize>>>,

    handle_counter: AtomicU64,
    handle_map: Arc<RwLock<HashMap<Handle, u64>>>,
    receipt_index_map: Arc<RwLock<HashMap<(Handle, usize), Vec<usize>>>>,
    receipt_index_counter: Arc<RwLock<HashMap<Handle, usize>>>,
}


impl PSLStorageConnector {
    pub async fn new(conn_url: String) -> Result<Self, LedgerStoreError> {
        let channel = Channel::from_shared(conn_url).unwrap().connect().await.unwrap();
        let connection = PslStorageCallClient::new(channel);
        let view_handle = match NimbleDigest::from_bytes(&vec![0u8; NimbleDigest::num_bytes()]) {
            Ok(e) => e,
            Err(_) => {
                return Err(LedgerStoreError::LedgerError(
                    StorageError::DeserializationError,
                ));
            },
        };

        let res = Self {
            connection: Arc::new(RwLock::new(connection)),
            view_handle: view_handle.clone(),
            tail_index: Arc::new(Mutex::new(HashMap::new())),
            handle_counter: AtomicU64::new(0),
            handle_map: Arc::new(RwLock::new(HashMap::new())),
            receipt_index_map: Arc::new(RwLock::new(HashMap::new())),
            receipt_index_counter: Arc::new(RwLock::new(HashMap::new())),
        };

        res.create_ledger(&view_handle, Block::new(&[0; 0])).await?;
        Ok(res)
    }


    fn get_handle_id(&self, handle: &Handle, is_for_receipts: bool, create_if_not_exists: bool) -> Option<u64> {
        let mut handle_map = self.handle_map.write().unwrap();
        
        let handle_id = if handle_map.contains_key(handle) {
            *handle_map.get(handle).unwrap()
        } else {
            if create_if_not_exists {
                let id = self.handle_counter.fetch_add(2, Ordering::SeqCst);
                    handle_map.insert(handle.clone(), id);
                    id
            } else {
                return None;
            }
        };

        if is_for_receipts {
            Some(handle_id + 1)
        } else {
            Some(handle_id)
        }
    }

    fn get_connection(&self) -> PslStorageCallClient<Channel> {
        let connection = self.connection.read().unwrap();
        connection.clone()
    }


    async fn store_remote(
        &self,
        handle: &Handle,
        block: &Block,
        idx: usize,
    ) -> Result<(), LedgerStoreError> {
        let seq_num = idx + 1;

        let handle_id = self.get_handle_id(handle, false, true).unwrap();
        let req = StoreRemoteReq {
            origin_id: handle_id,
            data: block.to_bytes(),
            seq_num: seq_num as u64,
        };

        let mut connection = self.get_connection();

        let res = connection.store_remote(req).await.map_err(|e| {
            LedgerStoreError::LedgerError(StorageError::SerializationError)
        })?;

        if !res.into_inner().success {
            return Err(LedgerStoreError::LedgerError(StorageError::DeserializationError));
        }

        Ok(())
    }

    async fn append_receipt(
        &self,
        handle: &Handle,
        idx: usize,
        receipt: &Receipts,
    ) -> Result<(), LedgerStoreError> {
        let seq_num = {
          let mut receipt_index_counter = self.receipt_index_counter.write().unwrap();
          let counter = receipt_index_counter.entry(handle.clone()).or_insert(0);
          *counter += 1;

          *counter
        };

        let handle_id = self.get_handle_id(handle, true, true).unwrap();
        let req = StoreRemoteReq {
            origin_id: handle_id,
            data: receipt.to_bytes(),
            seq_num: seq_num as u64,
        };
        
        let mut connection = self.get_connection();
        
        let res = connection.store_remote(req).await.map_err(|e| {
            LedgerStoreError::LedgerError(StorageError::SerializationError)
        })?;

        if !res.into_inner().success {
            return Err(LedgerStoreError::LedgerError(StorageError::DeserializationError));
        }

        let mut receipt_index_map = self.receipt_index_map.write().unwrap();
        let receipt_index = receipt_index_map.entry((handle.clone(), idx)).or_insert(Vec::new());
        receipt_index.push(seq_num as usize);

        Ok(())
    }

    async fn read_remote(
        &self,
        handle: &Handle,
        idx: usize,
    ) -> Result<LedgerEntry, LedgerStoreError> {
        // let seq_num = idx + 1;
        println!("!!!1 {}", idx);
        let mut entry = self._read_remote(handle, idx).await?;
        println!("!!!2");
        let receipts = self._read_all_receipts(handle, idx).await?;
        println!("!!!3");
        entry.receipts.merge_receipts(&receipts);
        Ok(entry)
    }

    async fn _read_remote(
        &self,
        handle: &Handle,
        idx: usize,
    ) -> Result<LedgerEntry, LedgerStoreError> {
        let seq_num = idx + 1;
        let handle_id = self.get_handle_id(handle, false, false);
        if handle_id.is_none() {
            return Err(LedgerStoreError::LedgerError(StorageError::KeyDoesNotExist));
        }

        let req = ReadRemoteReq {
            origin_id: handle_id.unwrap(),
            seq_num: seq_num as u64,
        };

        let mut connection = self.get_connection();
        let res = connection.read_remote(req).await.map_err(|e| {
            eprintln!("gRPC error: {:?} handle_id: {}", e, handle_id.unwrap());
            LedgerStoreError::LedgerError(StorageError::SerializationError)
        })?;

        let data = res.into_inner().data;
        // if data.is_empty() {
        //     return Err(LedgerStoreError::LedgerError(StorageError::KeyDoesNotExist));
        // }

        let entry = LedgerEntry::new(Block::from_bytes(&data).unwrap(), Receipts::new(), None);
        
        Ok(entry)
    }

    async fn _read_all_receipts(
        &self,
        handle: &Handle,
        idx: usize,
    ) -> Result<Receipts, LedgerStoreError> {
        let handle_id = self.get_handle_id(handle, true, false);
        if handle_id.is_none() {
            return Ok(Receipts::new());
        }
        let receipt_seq_nums = {
          let receipt_index_map = self.receipt_index_map.read().unwrap();
          let _default = Vec::new();
          let receipt_index = receipt_index_map.get(&(handle.clone(), idx)).unwrap_or(&_default);

          receipt_index.clone()
        };

        let mut receipts = Receipts::new();
        let mut connection = self.get_connection();
        for seq_num in receipt_seq_nums {
          let req = ReadRemoteReq {
              origin_id: handle_id.unwrap(),
              seq_num: seq_num as u64,
          };

          let res = connection.read_remote(req).await.map_err(|e| {
              LedgerStoreError::LedgerError(StorageError::SerializationError)
          })?;

          let data = res.into_inner().data;
          if data.is_empty() {
              return Err(LedgerStoreError::LedgerError(StorageError::KeyDoesNotExist));
          }

          let _receipts = Receipts::from_bytes(&data).unwrap();
          receipts.merge_receipts(&_receipts);
        }

        Ok(receipts)
    }
}


#[async_trait]
impl LedgerStore for PSLStorageConnector {
    async fn create_ledger(
        &self,
        handle: &NimbleDigest,
        genesis_block: Block,
      ) -> Result<(), LedgerStoreError>
      {
        self.append_ledger(handle, &genesis_block, 0).await?;
        Ok(())
      }
      async fn append_ledger(
        &self,
        handle: &Handle,
        block: &Block,
        expected_height: usize,
      ) -> Result<(usize, Nonces), LedgerStoreError>
      {
        self.store_remote(handle, block, expected_height).await?;

        let mut tail_index = self.tail_index.lock().await;
        tail_index.insert(handle.clone(), expected_height);

        Ok((expected_height, Nonces::new()))
      }
      async fn attach_ledger_receipts(
        &self,
        handle: &Handle,
        idx: usize,
        receipt: &Receipts,
      ) -> Result<(), LedgerStoreError>
      {
        self.append_receipt(handle, idx, receipt).await?;
        Ok(())
      }

      #[allow(unused_variables)]
      async fn attach_ledger_nonce(
        &self,
        handle: &Handle,
        nonce: &Nonce,
      ) -> Result<usize, LedgerStoreError>
      {
        unimplemented!()
      }
      async fn read_ledger_tail(
        &self,
        handle: &Handle,
      ) -> Result<(LedgerEntry, usize), LedgerStoreError>
      {
        let tail_index = {
            let tail_index = self.tail_index.lock().await;
            *tail_index.get(handle).ok_or(LedgerStoreError::LedgerError(
                StorageError::InvalidIndex
            ))?
        };
        
        let res = self.read_ledger_by_index(handle, tail_index).await?;
        Ok((res, tail_index))
      }
      async fn read_ledger_by_index(
        &self,
        handle: &Handle,
        idx: usize,
      ) -> Result<LedgerEntry, LedgerStoreError>
      {
        println!(">>>>>>>>>>>>>> Reading handle {:?} by index {}", handle, idx);
        self.read_remote(handle, idx).await
      }
      async fn append_view_ledger(
        &self,
        block: &Block,
        expected_height: usize,
      ) -> Result<usize, LedgerStoreError>
      {
        let res = self.append_ledger(&self.view_handle, block, expected_height).await?;
        Ok(res.0)
      }
      async fn attach_view_ledger_receipts(
        &self,
        idx: usize,
        receipt: &Receipts,
      ) -> Result<(), LedgerStoreError>
      {
        self.attach_ledger_receipts(&self.view_handle, idx, receipt).await?;
        Ok(())
      }
      async fn read_view_ledger_tail(&self) -> Result<(LedgerEntry, usize), LedgerStoreError>
      {
        self.read_ledger_tail(&self.view_handle).await
      }
      async fn read_view_ledger_by_index(&self, idx: usize) -> Result<LedgerEntry, LedgerStoreError>
      {
        println!(">>>>>>>>>>>>>> Reading view ledger by index {}", idx);
        self.read_ledger_by_index(&self.view_handle, idx).await
      }
    
      async fn reset_store(&self) -> Result<(), LedgerStoreError> // only used for testing
      {
        Ok(())
      }
}