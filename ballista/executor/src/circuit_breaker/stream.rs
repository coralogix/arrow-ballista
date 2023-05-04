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

use super::client::CircuitBreakerClient;

pub struct CircuitBreakerStream {
    inner: Pin<Box<dyn RecordBatchStream + Send>>,
    calculate: Arc<dyn Fn(&RecordBatch) -> f64 + Sync + Send>,
    task_id: String,
    partition: u32,
    percent: f64,
    is_lagging: bool,
    circuit_breaker: Arc<AtomicBool>,
    client: Arc<CircuitBreakerClient>,
}

impl CircuitBreakerStream {
    pub fn new(
        inner: Pin<Box<dyn RecordBatchStream + Send>>,
        calculate: Arc<dyn Fn(&RecordBatch) -> f64 + Sync + Send>,
        task_id: String,
        partition: u32,
        circuit_breaker: Arc<AtomicBool>,
        client: Arc<CircuitBreakerClient>,
    ) -> Self {
        Self {
            inner,
            calculate,
            task_id,
            partition,
            percent: 0.0,
            is_lagging: false,
            circuit_breaker,
            client,
        }
    }
}

#[derive(Debug)]
pub struct CircuitBreakerUpdate {
    pub task_id: String,
    pub partition: u32,
    pub percent: f64,
}

impl RecordBatchStream for CircuitBreakerStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for CircuitBreakerStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.circuit_breaker.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }

        let poll = self.inner.poll_next_unpin(cx);

        if let Poll::Ready(Some(Ok(record_batch))) = &poll {
            let delta = (self.calculate)(record_batch);

            self.percent += delta;

            let status_update = CircuitBreakerUpdate {
                task_id: self.task_id.clone(),
                partition: self.partition,
                percent: self.percent,
            };

            if let Err(e) = self.client.send_update(status_update) {
                if !self.is_lagging {
                    self.is_lagging = true;
                    warn!("Stream could not send short circuit update to daemon, it might be running very fast! ({:?})", e);
                }
            }
        }

        poll
    }
}
