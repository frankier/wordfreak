use std::collections::BTreeMap;
use itertools::Itertools;
use std::convert::TryInto;
use std::io::BufRead;
use std::str;
use std::io::BufReader;
use fnv::FnvHashMap;
use std::fs::{create_dir_all, File};


pub type VocabMap = FnvHashMap<Box<[u8]>, u32>;


// XXX: Reduce allocations using SmartString/SmallVec
pub struct VocabBuilder {
    pub acc: BTreeMap::<Box<[u8]>, u32>
}

impl VocabBuilder {
    pub fn new() -> VocabBuilder {
        VocabBuilder {
            acc: BTreeMap::<Box<[u8]>, u32>::new()
        }
    }

    fn inc_key_ref(&mut self, key: &[u8], inc: u32) {
        /*
        XXX: Switch to raw_entry API when supported

        Could use something like...
        if !self.acc.contains_key(key) {
            self.acc.insert(key.into(), inc);
        }
        */
        self.inc_key_owned(key.into(), inc);
    }

    fn inc_key_owned(&mut self, key: Box<[u8]>, inc: u32) {
        *self.acc.entry(key).or_insert(0) += inc;
    }

    pub fn add(&mut self, elem: &[u8]) {
        self.inc_key_ref(elem, 1);
    }

    pub fn merge(&mut self, other: VocabBuilder) {
        for (elem, cnt) in other.acc.into_iter() {
            self.inc_key_owned(elem, cnt);
        }
    }

    pub fn build(self) -> (VocabMap, Vec<u32>, u32) {
        let mut word_freqs_strings = self.acc
            .into_iter()
            .collect_vec();
        word_freqs_strings.sort_unstable_by(
                |(word_a, freq_a), (word_b, freq_b)| {
                    freq_b
                        .partial_cmp(freq_a)
                        .unwrap()
                        .then_with(|| word_a.partial_cmp(word_b).unwrap())
                });
        let mut vocab: VocabMap = VocabMap::default();
        let mut word_freqs_indexed = Vec::with_capacity(word_freqs_strings.len());
        let mut total_words: u32 = 0;
        for (idx, (word, cnt)) in word_freqs_strings.into_iter().enumerate() {
            vocab.insert(word, (idx as u32).try_into().unwrap());
            word_freqs_indexed.push(cnt);
            total_words += cnt;
        }
        (vocab, word_freqs_indexed, total_words)
    }
}


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
/*
fn merge_many(vocabs: &mut [Vocab]) -> Vocab {
    let mut acc = vocabs[0];
    for rest in vocabs[1..].iter() {
        acc.merge(rest);
    }
    acc
}
*/
