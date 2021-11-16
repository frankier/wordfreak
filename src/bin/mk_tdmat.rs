use std::env;
use std::path::Path;
use std::iter::Iterator;
use std::collections::BTreeMap;
use crossbeam_channel::unbounded;
use std::thread;
use wordfreak::termdocmat::{get_numberbatch_vocab, TermDocMatWriter};
use wordfreak::opensubs18::{mmap_file, iter_subtitles_whole_file, next_opensubs_doc_token, filter_xml_entries};
use rayon::prelude::*;
use rayon::current_num_threads;
use piz::ZipArchive;


static LEMMA_KEY: &[u8] = b"lemma";


fn main() {
    println!("Processing using {} threads", current_num_threads());
    let args: Vec<String> = env::args().collect();
    println!("Reading vocab");
    let vocab = get_numberbatch_vocab(&args[2]);
    println!("Vocab size: {}", vocab.len());

    println!("Reading and writing other files");
    let out_dir = Path::new(&args[3]);

    let (sender, receiver) = unbounded();

    let mut writer = TermDocMatWriter::new(out_dir, vocab.len() as u64);

    let pipe_reader = thread::spawn(move || {
        while let Some((doc_words, counts)) = receiver.recv().unwrap() {
            writer.write_indexed_doc(doc_words, &counts);
        }
        let (num_docs, vocab_len, num_values) = writer.close();
        println!("Vocab size: {}", vocab_len);
        println!("Num docs: {}", num_docs);
        println!("Num values: {}", num_values);
        println!("Density: {}", (num_values as f64) / ((num_docs * (vocab_len as u64))) as f64);
    });

    let mmap = mmap_file(Path::new(&args[1]));
    let zip_reader: ZipArchive = ZipArchive::new(&mmap).unwrap();
    let xml_entries = filter_xml_entries(&zip_reader);
    iter_subtitles_whole_file(&xml_entries, &mmap).for_each_init(
        || Vec::<u8>::new(), |mut xml_read_buf, mut reader| {
            let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
            // XXX: Could have some kind of pool for these
            let mut doc_words = 0;
            loop {
                let got_some = next_opensubs_doc_token(&mut xml_read_buf, &mut reader, LEMMA_KEY, |lemma| {
                    let maybe_vocab_idx = vocab.get(lemma);
                    if let Some(vocab_idx) = maybe_vocab_idx {
                        *counts.entry(*vocab_idx).or_insert(0) += 1;
                        doc_words += 1;
                    }
                });
                if got_some == None {
                    break;
                }
            }
            sender.send(Some((doc_words, counts))).unwrap();
        }
    );
    sender.send(None).unwrap();
    pipe_reader.join().unwrap()
}
