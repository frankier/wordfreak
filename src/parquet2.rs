use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::iter::once;
use std::str::from_utf8;
use itertools::Itertools;

use arrow2::array::{Array, Utf8Array, UInt32Array, Float64Array};
use arrow2::datatypes::{Field, Schema, DataType};
use arrow2::io::parquet::write::{
    write_file, Compression, Encoding, Version, WriteOptions, RowGroupIterator
};
use arrow2::record_batch::RecordBatch;


fn get_schema(cols: &[&str]) -> Schema {
    let word = Field::new("word", DataType::Utf8, false);
    let count = Field::new("count", DataType::UInt32, false);
    let mut fields = vec![word, count];

    fields.extend(cols.iter().map(|col| Field::new(col, DataType::Float64, false)));
    Schema::new(fields)
}

pub fn write_parquet(out_path: &Path, words: &[Box<[u8]>], counts: &[u32], col_names: &[&str], cols: &[&[f64]]) {
    let mut col_arrays: Vec<Arc<dyn Array>> = vec![
        Arc::new(Utf8Array::<i32>::from_iter_values(words.into_iter().map(|b| from_utf8(b).unwrap()))),
        Arc::new(UInt32Array::from_slice(counts)),
    ];
    col_arrays.extend(cols.into_iter().map(|col| Arc::new(Float64Array::from_slice(col)) as Arc<dyn Array>));
    for col_array in col_arrays.iter() {
        println!("col_array len {}", (*col_array).len())
    }
    let batch = RecordBatch::try_new(Arc::new(get_schema(col_names)), col_arrays).unwrap();

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Zsld,
        version: Version::V2,
    };

    let schema = batch.schema().clone();
    let encodings = schema.fields().iter().map(|_| Encoding::Plain).collect_vec();
    let row_groups = RowGroupIterator::try_new(once(Ok(batch)), &schema, options, encodings).unwrap();
    let parquet_schema = row_groups.parquet_schema().clone();

    // Create a new empty file
    let mut file = File::create(out_path).unwrap();

    // Write the file. Note that, at present, any error results in a corrupted file.
    let _ = write_file(
        &mut file,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    ).unwrap();
}
