/*!
 * MessageRecord Indexing
 */

use super::MessageRecord;
use anyhow::Result;
use common::util::{check_and_create_dir, check_exist};
use std::sync::atomic::Ordering::Relaxed;
use std::{
    cell::RefCell,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64},
};
use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Field, Schema, Value as _, STORED, TEXT},
    Index as TanIndex, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument,
};

pub trait Index: Send + Sync {
    fn push(&self, record: MessageRecord) -> Result<()>;
    fn find(&self, id: &str) -> Result<Option<MessageRecord>>;
}

pub struct IndexTantivy {
    dir: PathBuf,
    budget_in_bytes: usize,

    // flush
    flush_factor_target: AtomicU64,
    flush_factor_now: AtomicU64,
    need_flush: AtomicBool,

    // index
    index: RefCell<Option<IndexWrapper>>,
}

unsafe impl Send for IndexTantivy {}
unsafe impl Sync for IndexTantivy {}

impl IndexTantivy {
    pub fn new(dir: PathBuf, budget_in_bytes: usize) -> Result<Self> {
        Ok(IndexTantivy {
            dir,
            budget_in_bytes,
            flush_factor_target: AtomicU64::new(0),
            flush_factor_now: AtomicU64::new(0),
            need_flush: AtomicBool::new(false),
            index: RefCell::new(None),
        })
    }

    fn init_index(&self) -> Result<()> {
        let mut index = self.index.borrow_mut();
        if index.is_some() {
            return Ok(());
        }
        let index_wrapper = IndexWrapper::new(self.dir.clone(), self.budget_in_bytes)?;
        *index = Some(index_wrapper);
        Ok(())
    }
}

impl Index for IndexTantivy {
    fn push(&self, record: MessageRecord) -> Result<()> {
        self.init_index()?;

        let index = self.index.borrow_mut();
        index.as_ref().unwrap().push(record)?;
        self.flush_factor_now.fetch_add(1, Relaxed);
        if self.flush_factor_now.load(Relaxed) >= self.flush_factor_target.load(Relaxed) {
            index.as_ref().unwrap().writer_commit()?;
            self.flush_factor_now.store(0, Relaxed);
            self.need_flush.store(false, Relaxed);
            return Ok(());
        }
        self.need_flush.store(true, Relaxed);

        Ok(())
    }

    fn find(&self, id: &str) -> Result<Option<MessageRecord>> {
        let index = self.index.borrow_mut();
        if index.is_none() {
            return Ok(None);
        }
        if self.need_flush.load(Relaxed) {
            index.as_ref().unwrap().writer_commit()?;
            self.need_flush.store(false, Relaxed);
        }

        index.as_ref().unwrap().find(id)
    }
}

struct IndexWrapper {
    schema: Schema,
    fields: Vec<Field>,

    need_reload: AtomicBool,
    // index
    index: TanIndex,
    writer: RefCell<IndexWriter>,
    reader: IndexReader,
}

impl IndexWrapper {
    fn new(dir: PathBuf, budget_in_bytes: usize) -> Result<Self> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("record", TEXT | STORED);
        let schema = schema_builder.build();
        let record = schema.get_field("record")?;

        if check_exist(&dir) {
            let index = TanIndex::open_in_dir(dir.clone())?;
            return Ok(IndexWrapper {
                schema,
                fields: vec![record],
                need_reload: AtomicBool::new(false),
                writer: RefCell::new(index.writer(budget_in_bytes)?),
                reader: index.reader()?,
                index,
            });
        }

        check_and_create_dir(&dir)?;
        let index = TanIndex::create_in_dir(dir.clone(), schema.clone())?;

        Ok(IndexWrapper {
            schema,
            fields: vec![record],
            need_reload: AtomicBool::new(false),
            writer: index.writer(budget_in_bytes)?.into(),
            reader: index
                .reader_builder()
                .reload_policy(ReloadPolicy::OnCommitWithDelay)
                .try_into()?,
            index,
        })
    }

    fn push(&self, record: MessageRecord) -> Result<()> {
        let writer = self.writer.borrow_mut();
        writer.add_document(doc! {
            *self.fields.first().unwrap() => record.format()
        })?;

        Ok(())
    }

    fn find(&self, id: &str) -> Result<Option<MessageRecord>> {
        if self.need_reload.load(Relaxed) {
            self.reader.reload()?;
            self.need_reload.store(false, Relaxed);
        }

        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, self.fields.clone());
        let query = query_parser.parse_query(id)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        if top_docs.is_empty() {
            return Ok(None);
        }
        let (_, doc_addr) = top_docs.first().unwrap();
        let record_doc: TantivyDocument = searcher.doc(*doc_addr).unwrap();
        let record_content = record_doc
            .get_first(*self.fields.first().unwrap())
            .unwrap()
            .as_str()
            .unwrap();

        let record = MessageRecord::parse_from(record_content)?;

        Ok(Some(record))
    }

    fn writer_commit(&self) -> Result<()> {
        self.writer.borrow_mut().commit()?;
        self.need_reload.store(true, Relaxed);
        Ok(())
    }
}
