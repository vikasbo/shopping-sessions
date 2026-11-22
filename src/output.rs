//! Parquet writers for the two output tables (arrow-rs).

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::borrow::Cow;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::sessions::URow;

fn s(b: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(b) // borrows when the bytes are valid UTF-8 (always, per rules.md)
}

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build()
}

fn write(path: &Path, batch: RecordBatch) {
    let file = File::create(path).expect("create parquet file");
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props())).expect("parquet writer");
    w.write(&batch).expect("write batch");
    w.close().expect("close parquet");
}

pub fn write_users(path: &Path, rows: &[URow], cat_names: &[Vec<u8>]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| s(r.person)))),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.day))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.sessions as i64))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.events as i64))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.revenue as i64))),
        Arc::new(rows.iter().map(|r| r.top.map(|c| s(&cat_names[c as usize]))).collect::<StringArray>()),
    ];
    write(path, RecordBatch::try_new(schema, cols).expect("user batch"));
}

pub struct CRow<'a> {
    pub campaign: &'a [u8],
    pub channel: &'a [u8],
    pub day: i32,
    pub v: [u64; 4],
}

pub fn write_campaigns(path: &Path, rows: &[CRow]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let int = |k: usize| -> ArrayRef { Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.v[k] as i64))) };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| s(r.campaign)))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| s(r.channel)))),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.day))),
        int(0),
        int(1),
        int(2),
        int(3),
    ];
    write(path, RecordBatch::try_new(schema, cols).expect("campaign batch"));
}
