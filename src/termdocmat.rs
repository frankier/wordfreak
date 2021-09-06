use std::io::prelude::*;
use std::io::BufWriter;
use std::io::BufRead;
use std::path::Path;
use std::str;
use std::fs::{create_dir_all, File};
use std::io::BufReader;

use fnv::FnvHashMap;

pub type VocabMap = FnvHashMap<Box<[u8]>, u32>;


pub fn get_numberbatch_vocab(in_path: &str) -> VocabMap {
    // XXX: Inefficient: reads a bunch of stuff just to throw it away and then copies the vocab
    let file = File::open(in_path).unwrap();
    let mut reader = BufReader::new(file);
    let mut buf = Vec::<u8>::with_capacity(64);
    let mut vocab = FnvHashMap::<Box<[u8]>, u32>::default();
    let mut idx = 0;
    loop {
        let read_bytes = reader.read_until(b' ', &mut buf).unwrap();
        if read_bytes == 0 {
            break;
        }
        vocab.insert(buf.as_slice()[..buf.len()-1].to_owned().into_boxed_slice(), idx);
        buf.clear();
        // XXX: Strictly we would prefer to have a discard_until
        let read_bytes = reader.read_until(b'\n', &mut buf).unwrap();
        if read_bytes == 0 {
            break;
        }
        buf.clear();
        idx += 1;
    }
    return vocab;
}

pub struct TermDocMatWriter {
    vocab_len: u64,
    num_docs: u64,
    num_values: u64,
    data_counts: BufWriter<File>,
    data_norm: BufWriter<File>,
    indices: BufWriter<File>,
    indptr: BufWriter<File>,
    dims: File,
}

impl TermDocMatWriter {
    pub fn new(out_dir: &Path, vocab_len: u64) -> TermDocMatWriter {
        create_dir_all(&out_dir).unwrap();
        let data_counts = BufWriter::new(File::create(out_dir.join("data_counts")).unwrap());
        let data_norm = BufWriter::new(File::create(out_dir.join("data_norm")).unwrap());
        let indices = BufWriter::new(File::create(out_dir.join("indices")).unwrap());
        let indptr = BufWriter::new(File::create(out_dir.join("indptr")).unwrap());
        let dims = File::create(out_dir.join("indptr")).unwrap();

        TermDocMatWriter {
            vocab_len,
            num_docs: 0,
            num_values: 0,
            data_counts,
            data_norm,
            indices,
            indptr,
            dims,
        }
    }

    pub fn write_indexed_doc<'a, I: 'a>(&mut self, doc_words: u64, counts: &'a I) where &'a I: IntoIterator<Item=(&'a u32, &'a u32)> {
        self.indptr.write(&self.num_values.to_le_bytes()).unwrap();
        let mut total: f64 = 0.0;
        for (col, val) in counts {
            self.indices.write(&col.to_le_bytes()).unwrap();
            self.data_counts.write(&val.to_le_bytes()).unwrap();
            total += (val * val) as f64;
        }
        let total_sqrt = total.sqrt();
        for (_, val) in counts {
            self.data_norm.write(&(((*val as f64) / total_sqrt) as f32).to_le_bytes()).unwrap();
        }
        self.num_values += doc_words;
        self.num_docs += 1;
    }

    pub fn close(mut self) -> (u64, u64, u64) {
        self.indptr.write(&self.num_values.to_le_bytes()).unwrap();
        self.data_counts.flush().unwrap();
        self.data_norm.flush().unwrap();
        self.indices.flush().unwrap();
        self.indptr.flush().unwrap();
        self.dims.write(&(self.num_docs as u64).to_le_bytes()).unwrap();
        self.dims.write(&(self.vocab_len as u64).to_le_bytes()).unwrap();
        self.dims.flush().unwrap();
        (self.num_docs, self.vocab_len, self.num_values)
    }
}
