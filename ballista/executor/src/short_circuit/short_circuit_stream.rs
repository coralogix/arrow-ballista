use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use tracing::warn;

use super::short_circuit_client::ShortCircuitClient;

#[derive(Debug)]
pub struct ShortCircuitConfig {
    pub short_circuit: Arc<AtomicBool>,
}

pub struct ShortCircuitStream {
    pub inner: Pin<Box<dyn RecordBatchStream + Send>>,
    pub config: ShortCircuitConfig,
    pub client: Arc<ShortCircuitClient>,
    pub task_id: String,
    pub partition: usize,
    count_mode: CountMode,
    buffered_count: u64,
}

pub enum CountMode {
    Bytes,
    Rows,
}

impl ShortCircuitStream {
    pub fn new(
        inner: Pin<Box<dyn RecordBatchStream + Send>>,
        config: ShortCircuitConfig,
        client: Arc<ShortCircuitClient>,
        task_id: String,
        partition: usize,
        count_mode: CountMode,
    ) -> Self {
        Self {
            inner,
            config,
            client,
            task_id,
            partition,
            count_mode,
            buffered_count: 0,
        }
    }
}

#[derive(Debug)]
pub struct ShortCircuitUpdate {
    pub task_id: String,
    pub partition: usize,
    pub count: u64,
}

impl RecordBatchStream for ShortCircuitStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for ShortCircuitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.config.short_circuit.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }

        let poll = self.inner.poll_next_unpin(cx);

        if let Poll::Ready(Some(Ok(record_batch))) = &poll {
            let count = match self.count_mode {
                CountMode::Bytes => record_batch.get_array_memory_size() as u64,
                CountMode::Rows => record_batch.num_rows() as u64,
            };

            let status_update = ShortCircuitUpdate {
                task_id: self.task_id.clone(),
                partition: self.partition,
                count,
            };

            if let Err(e) = self.client.send_update(status_update) {
                // So we only log when it starts happening
                if self.buffered_count == 0 {
                    warn!("Stream could not send short circuit update to daemon, it might be running very fast! ({:?})", e);
                }

                self.buffered_count += count;
            }
        }

        poll
    }
}
