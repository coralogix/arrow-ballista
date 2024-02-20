use std::{collections::HashMap, fmt, io::SeekFrom, sync::Arc};

use async_stream::stream;
use datafusion::{
    arrow::{
        array::ArrayRef,
        buffer::MutableBuffer,
        datatypes::{Schema, SchemaRef},
        error::ArrowError,
        ipc::{
            convert,
            reader::{read_dictionary, read_record_batch},
            root_as_footer, root_as_message, Block, MessageHeader, MetadataVersion,
        },
        record_batch::RecordBatch,
    },
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use futures::{
    future::BoxFuture, io::BufReader, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt,
};

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

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

    /// Optional projection and projected_schema
    projection: Option<(Vec<usize>, Schema)>,
}

impl<R: AsyncRead + AsyncSeek> fmt::Debug for AsyncFileReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("AsyncFileReader<R>")
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
            projection,
        })
    }

    /// Return the schema of the file
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub async fn maybe_next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if let Some(block) = self.blocks.get(self.current_block) {
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
                    "Could not read IPC message as metadata versions mismatch"
                        .to_string(),
                ));
            }

            match message.header_type() {
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

                    return read_record_batch(
                        &buf.into(),
                        batch,
                        self.schema(),
                        &self.dictionaries_by_id,
                        self.projection.as_ref().map(|x| x.0.as_ref()),
                        &message.version()

                    ).map(Some)
                }
                MessageHeader::NONE => return Ok(None),
                MessageHeader::Schema => return Err(ArrowError::IpcError(
                    "Not expecting a schema when messages are read".to_string(),
                )),
                t => return Err(ArrowError::InvalidArgumentError(format!(
                    "Reading types other than record batches not yet supported, unable to read {t:?}"
                ))),
        }
        }

        Ok(None)
    }
}

/// Arrow Stream reader
pub struct AsyncStreamReader<R: AsyncRead + Unpin + Send> {
    /// Stream reader
    reader: R,

    /// The schema that is read from the stream's first message
    schema: SchemaRef,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_id: HashMap<i64, ArrayRef>,

    /// An indicator of whether the stream is complete.
    ///
    /// This value is set to `true` the first time the reader's `next()` returns `None`.
    finished: bool,

    /// Optional projection
    projection: Option<(Vec<usize>, Schema)>,
}

impl<R: AsyncRead + Unpin + Send> fmt::Debug for AsyncStreamReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("AsyncStreamReader<R>")
            .field("reader", &"BufReader<..>")
            .field("schema", &self.schema)
            .field("dictionaries_by_id", &self.dictionaries_by_id)
            .field("finished", &self.finished)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncStreamReader<R> {
    /// Try to create a new stream reader but do not wrap the reader in a BufReader.
    ///
    /// Unless you need the AsyncStreamReader to be unbuffered you likely want to use `AsyncStreamReader::try_new` instead.
    pub async fn try_new_unbuffered(
        mut reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<AsyncStreamReader<R>, ArrowError> {
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];
        reader.read_exact(&mut meta_size).await?;
        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == CONTINUATION_MARKER {
                reader.read_exact(&mut meta_size).await?;
            }
            i32::from_le_bytes(meta_size)
        };

        let mut meta_buffer = vec![0; meta_len as usize];
        reader.read_exact(&mut meta_buffer).await?;

        let message = root_as_message(meta_buffer.as_slice()).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
        })?;
        // message header is a Schema, so read it
        let ipc_schema = message.header_as_schema().ok_or_else(|| {
            ArrowError::ParseError("Unable to read IPC message as schema".to_string())
        })?;
        let schema = convert::fb_to_schema(ipc_schema);

        // Create an array of optional dictionary value arrays, one per field.
        let dictionaries_by_id = HashMap::new();

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
            finished: false,
            dictionaries_by_id,
            projection,
        })
    }

    /// Return the schema of the stream
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn maybe_next(
        &mut self,
    ) -> BoxFuture<'_, Result<Option<RecordBatch>, ArrowError>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            // determine metadata length
            let mut meta_size: [u8; 4] = [0; 4];

            if let Err(e) = self.reader.read_exact(&mut meta_size).await {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // Handle EOF without the "0xFFFFFFFF 0x00000000"
                    // valid according to:
                    // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                    self.finished = true;
                    Ok(None)
                } else {
                    Err(ArrowError::from(e))
                };
            }

            let meta_len = {
                // If a continuation marker is encountered, skip over it and read
                // the size from the next four bytes.
                if meta_size == CONTINUATION_MARKER {
                    self.reader.read_exact(&mut meta_size).await?;
                }
                i32::from_le_bytes(meta_size)
            };

            if meta_len == 0 {
                // the stream has ended, mark the reader as finished
                self.finished = true;
                return Ok(None);
            }

            let mut meta_buffer = vec![0; meta_len as usize];
            self.reader.read_exact(&mut meta_buffer).await?;

            let vecs = &meta_buffer.to_vec();
            let message = root_as_message(vecs).map_err(|err| {
                ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
            })?;

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
                self.reader.read_exact(&mut buf).await?;

                read_record_batch(&buf.into(), batch, self.schema(), &self.dictionaries_by_id, self.projection.as_ref().map(|x| x.0.as_ref()), &message.version()).map(Some)
            }
            MessageHeader::DictionaryBatch => {
                let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    ArrowError::IpcError(
                        "Unable to read IPC message as dictionary batch".to_string(),
                    )
                })?;
                // read the block that makes up the dictionary batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf).await?;

                read_dictionary(
                    &buf.into(), batch, &self.schema, &mut self.dictionaries_by_id, &message.version()
                )?;

                // read the next message until we encounter a RecordBatch
                self.maybe_next().await
            }
            MessageHeader::NONE => {
                Ok(None)
            }
            t => Err(ArrowError::InvalidArgumentError(
                format!("Reading types other than record batches not yet supported, unable to read {t:?} ")
            )),
        }
        })
    }

    /// Gets a reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Gets a mutable reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncStreamReader<R> {
    pub fn to_stream(mut self) -> SendableRecordBatchStream {
        let schema = self.schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream! {
                while let Some(batch) = self.maybe_next().await.transpose() {
                    yield batch.map_err(|err| err.into())
                }
            },
        ))
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncStreamReader<BufReader<R>> {
    /// Try to create a new stream reader with the reader wrapped in a BufReader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub async fn try_new(
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        Self::try_new_unbuffered(BufReader::new(reader), projection).await
    }
}
