use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::buffer::MutableBuffer;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::{read_dictionary, read_record_batch};
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::ipc::{
    convert, root_as_footer, root_as_message, Block, CompressionType, MessageHeader,
    MetadataVersion,
};

use datafusion::arrow::record_batch::RecordBatch;
use futures::io::BufReader;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, sync::mpsc};
use tracing::{info, warn};

use crate::error::BallistaError;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
pub const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

pub enum Command {
    Replicate { path: String },
}

pub async fn start_replication(
    base_path: String,
    object_store: Arc<dyn ObjectStore>,
    mut receiver: mpsc::Receiver<Command>,
) -> Result<(), BallistaError> {
    while let Some(Command::Replicate { path }) = receiver.recv().await {
        let destination = format!("{}/{}", base_path, path);
        info!(destination, "Start replication");
        match Path::parse(destination) {
            Ok(dest) => match File::open(path.as_str()).await {
                Ok(file) => match AsyncFileReader::try_new(file.compat(), None).await {
                    Ok(mut reader) => match object_store.put_multipart(&dest).await {
                        Ok((_, mut multipart_writer)) => {
                            while let Some(batch) = reader.maybe_next().await.transpose()
                            {
                                match batch {
                                    Ok(batch) => {
                                        let mut serialized_data = Vec::with_capacity(
                                            batch.get_array_memory_size(),
                                        );
                                        let options = match IpcWriteOptions::default()
                                            .try_with_compression(Some(
                                                CompressionType::LZ4_FRAME,
                                            )) {
                                            Err(error) => {
                                                warn!(
                                                    ?error,
                                                    "Failed to enable compression"
                                                );
                                                IpcWriteOptions::default()
                                            }
                                            Ok(opt) => opt,
                                        };

                                        {
                                            match StreamWriter::try_new_with_options(
                                                &mut serialized_data,
                                                batch.schema().as_ref(),
                                                options,
                                            ) {
                                                Ok(mut writer) => {
                                                    if let Err(error) =
                                                        writer.write(&batch)
                                                    {
                                                        warn!(?error, ?path, "Failed to write batch to a stream writer");
                                                        break;
                                                    }

                                                    if let Err(error) = writer.finish() {
                                                        warn!(
                                                            ?error,
                                                            ?path,
                                                            "Failed to finish writing to a stream writer");
                                                        break;
                                                    }
                                                }
                                                Err(error) => {
                                                    warn!(
                                                        ?error,
                                                        ?path,
                                                        "Failed to build a stream writer"
                                                    );
                                                    break;
                                                }
                                            }
                                        }

                                        let bytes = Bytes::from(serialized_data);
                                        if let Err(error) =
                                            multipart_writer.write_all(&bytes).await
                                        {
                                            warn!(
                                                ?error,
                                                ?path,
                                                "Failed to write batch to object store"
                                            );
                                            break;
                                        }
                                    }
                                    Err(error) => {
                                        warn!(
                                            ?error,
                                            ?path,
                                            "Failed to write retrieve batch from stream"
                                        );
                                        break;
                                    }
                                }
                            }

                            if let Err(error) = multipart_writer.flush().await {
                                warn!(?error, ?path, "Failed to flush writer")
                            }
                            if let Err(error) = multipart_writer.shutdown().await {
                                warn!(?error, ?path, "Failed to shutdown writer")
                            }
                        }
                        Err(error) => {
                            warn!(
                                ?error,
                                ?path,
                                "Failed to create obejct store multipart writer"
                            );
                        }
                    },
                    Err(error) => {
                        warn!(?error, ?path, "Failed to create async reader");
                    }
                },
                Err(error) => {
                    warn!(?error, ?path, "Failed to open file for replication");
                }
            },
            Err(error) => {
                warn!(?error, ?path, "Failed to parse replication path");
            }
        }
    }

    Ok(())
}

/// Arrow File reader
pub struct AsyncFileReader<R: AsyncRead + AsyncSeek> {
    /// Buffered file reader that supports reading and seeking
    reader: BufReader<R>,

    /// The schema that is read from the file header
    schema: SchemaRef,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<Block>,

    /// A counter to keep track of the current block that should be read
    current_block: usize,

    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_id: HashMap<i64, ArrayRef>,

    /// Metadata version
    metadata_version: MetadataVersion,

    /// User defined metadata
    custom_metadata: HashMap<String, String>,

    /// Optional projection and projected_schema
    projection: Option<(Vec<usize>, Schema)>,
}

impl<R: AsyncRead + AsyncSeek> fmt::Debug for AsyncFileReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("FileReader<R>")
            .field("reader", &"BufReader<..>")
            .field("schema", &self.schema)
            .field("blocks", &self.blocks)
            .field("current_block", &self.current_block)
            .field("total_blocks", &self.total_blocks)
            .field("dictionaries_by_id", &self.dictionaries_by_id)
            .field("metadata_version", &self.metadata_version)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncFileReader<R> {
    /// Try to create a new file reader
    ///
    /// Returns errors if the file does not meet the Arrow Format header and footer
    /// requirements
    pub async fn try_new(
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        let mut reader = BufReader::new(reader);
        // check if header and footer contain correct magic bytes
        let mut magic_buffer: [u8; 6] = [0; 6];
        reader.read_exact(&mut magic_buffer).await?;
        if magic_buffer != ARROW_MAGIC {
            return Err(ArrowError::ParseError(
                "Arrow file does not contain correct header".to_string(),
            ));
        }
        reader.seek(SeekFrom::End(-6)).await?;
        reader.read_exact(&mut magic_buffer).await?;
        if magic_buffer != ARROW_MAGIC {
            return Err(ArrowError::ParseError(
                "Arrow file does not contain correct footer".to_string(),
            ));
        }
        // read footer length
        let mut footer_size: [u8; 4] = [0; 4];
        reader.seek(SeekFrom::End(-10)).await?;
        reader.read_exact(&mut footer_size).await?;
        let footer_len = i32::from_le_bytes(footer_size);

        // read footer
        let mut footer_data = vec![0; footer_len as usize];
        reader.seek(SeekFrom::End(-10 - footer_len as i64)).await?;
        reader.read_exact(&mut footer_data).await?;

        let footer = root_as_footer(&footer_data[..]).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as footer: {err:?}"))
        })?;

        let blocks = footer.recordBatches().ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to get record batches from IPC Footer".to_string(),
            )
        })?;

        let total_blocks = blocks.len();

        let ipc_schema = footer.schema().unwrap();
        let schema = convert::fb_to_schema(ipc_schema);

        let mut custom_metadata = HashMap::new();
        if let Some(fb_custom_metadata) = footer.custom_metadata() {
            for kv in fb_custom_metadata.into_iter() {
                custom_metadata.insert(
                    kv.key().unwrap().to_string(),
                    kv.value().unwrap().to_string(),
                );
            }
        }

        // Create an array of optional dictionary value arrays, one per field.
        let mut dictionaries_by_id = HashMap::new();
        if let Some(dictionaries) = footer.dictionaries() {
            for block in dictionaries {
                // read length from end of offset
                let mut message_size: [u8; 4] = [0; 4];
                reader.seek(SeekFrom::Start(block.offset() as u64)).await?;
                reader.read_exact(&mut message_size).await?;
                if message_size == CONTINUATION_MARKER {
                    reader.read_exact(&mut message_size).await?;
                }
                let footer_len = i32::from_le_bytes(message_size);
                let mut block_data = vec![0; footer_len as usize];

                reader.read_exact(&mut block_data).await?;

                let message = root_as_message(&block_data[..]).map_err(|err| {
                    ArrowError::ParseError(format!(
                        "Unable to get root as message: {err:?}"
                    ))
                })?;

                match message.header_type() {
                    MessageHeader::DictionaryBatch => {
                        let batch = message.header_as_dictionary_batch().unwrap();

                        // read the block that makes up the dictionary batch into a buffer
                        let mut buf =
                            MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                        reader
                            .seek(SeekFrom::Start(
                                block.offset() as u64 + block.metaDataLength() as u64,
                            ))
                            .await?;
                        reader.read_exact(&mut buf).await?;

                        read_dictionary(
                            &buf.into(),
                            batch,
                            &schema,
                            &mut dictionaries_by_id,
                            &message.version(),
                        )?;
                    }
                    t => {
                        return Err(ArrowError::ParseError(format!(
                            "Expecting DictionaryBatch in dictionary blocks, found {t:?}."
                        )));
                    }
                }
            }
        }
        let projection = match projection {
            Some(projection_indices) => {
                let schema = schema.project(&projection_indices)?;
                Some((projection_indices, schema))
            }
            _ => None,
        };

        Ok(Self {
            reader,
            schema: Arc::new(schema),
            blocks: blocks.iter().copied().collect(),
            current_block: 0,
            total_blocks,
            dictionaries_by_id,
            metadata_version: footer.version(),
            custom_metadata,
            projection,
        })
    }

    /// Return user defined customized metadata
    pub fn custom_metadata(&self) -> &HashMap<String, String> {
        &self.custom_metadata
    }

    /// Return the number of batches in the file
    pub fn num_batches(&self) -> usize {
        self.total_blocks
    }

    /// Return the schema of the file
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Read a specific record batch
    ///
    /// Sets the current block to the index, allowing random reads
    pub fn set_index(&mut self, index: usize) -> Result<(), ArrowError> {
        if index >= self.total_blocks {
            Err(ArrowError::InvalidArgumentError(format!(
                "Cannot set batch to index {} from {} total batches",
                index, self.total_blocks
            )))
        } else {
            self.current_block = index;
            Ok(())
        }
    }

    async fn maybe_next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let block = self.blocks[self.current_block];
        self.current_block += 1;

        // read length
        self.reader
            .seek(SeekFrom::Start(block.offset() as u64))
            .await?;
        let mut meta_buf = [0; 4];
        self.reader.read_exact(&mut meta_buf).await?;
        if meta_buf == CONTINUATION_MARKER {
            // continuation marker encountered, read message next
            self.reader.read_exact(&mut meta_buf).await?;
        }
        let meta_len = i32::from_le_bytes(meta_buf);

        let mut block_data = vec![0; meta_len as usize];
        self.reader.read_exact(&mut block_data).await?;
        let message = root_as_message(&block_data[..]).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as footer: {err:?}"))
        })?;

        // some old test data's footer metadata is not set, so we account for that
        if self.metadata_version != MetadataVersion::V1
            && message.version() != self.metadata_version
        {
            return Err(ArrowError::IpcError(
                "Could not read IPC message as metadata versions mismatch".to_string(),
            ));
        }

        match message.header_type() {
            MessageHeader::Schema => Err(ArrowError::IpcError(
                "Not expecting a schema when messages are read".to_string(),
            )),
            MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IpcError(
                        "Unable to read IPC message as record batch".to_string(),
                    )
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.seek(SeekFrom::Start(
                    block.offset() as u64 + block.metaDataLength() as u64,
                )).await?;
                self.reader.read_exact(&mut buf).await?;

                read_record_batch(
                    &buf.into(),
                    batch,
                    self.schema(),
                    &self.dictionaries_by_id,
                    self.projection.as_ref().map(|x| x.0.as_ref()),
                    &message.version()

                ).map(Some)
            }
            MessageHeader::NONE => {
                Ok(None)
            }
            t => Err(ArrowError::InvalidArgumentError(format!(
                "Reading types other than record batches not yet supported, unable to read {t:?}"
            ))),
        }
    }

    /// Gets a reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_ref(&self) -> &R {
        self.reader.get_ref()
    }

    /// Gets a mutable reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        self.reader.get_mut()
    }
}
