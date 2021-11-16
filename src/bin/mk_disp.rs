use std::path::Path;
use wordfreak::opensubs18::mmap_file;
use wordfreak::termdocmat::VocabMap;
use rayon::prelude::*;
use piz::ZipArchive;
use argh::FromArgs;
use std::str::FromStr;
use simple_error::SimpleError;
use std::collections::BTreeMap;
use itertools::Itertools;
use std::convert::TryInto;
use superslice::*;
use wordfreak::opensubs18::{iter_doc_bows, iter_flat_tokens, filter_xml_entries, MinEntries, mk_default_pool};
use wordfreak::parquet2::write_parquet;
use wordfreak::dispersion::{AccElement, acc_word, reduce_word, norm_word, FinalColumns};
use rayon::current_num_threads;
use memmap::Mmap;


enum CorpusType {
    OpenSubtitles2018,
    NewsCrawlWMT18
}

impl FromStr for CorpusType {
    type Err = SimpleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "opensubs18" {
            Ok(CorpusType::OpenSubtitles2018)
        } else if s == "newscrawl" {
            Ok(CorpusType::NewsCrawlWMT18)
        } else {
            Err(SimpleError::new("Must be opensubs18 or newscrawl"))
        }
    }
}


#[derive(FromArgs)]
/// add
struct MkDisp {
    /// type of corpus to use
    #[argh(option)]
    corpus_type: Option<CorpusType>,

    /// count lemmas rather than word forms
    #[argh(switch)]
    lemma: bool,

    /// path
    #[argh(positional)]
    output: String,

    /// path
    #[argh(positional)]
    input: Vec<String>,
}

static LEMMA_KEY: &[u8] = b"lemma";

/// Indexes the collection and at the same time collects counts per word, as well as the total
/// token count.
fn one_scan_index_count(xml_entries: &MinEntries, mmap: &Mmap) -> (VocabMap, Vec<u32>, u32) {
    /*
    let args: MkDisp = argh::from_env();
    let (sender, receiver) = unbounded();

    let mut writer = TermDocMatWriter::new(args.output, vocab.len() as u64);

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
    iter_subtitles(&zip_reader).for_each_init(
        || Vec::<u8>::new(), |mut xml_read_buf, mut reader| {
            let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
            let mut doc_words = 0;
            next_token(&mut xml_read_buf, &mut reader, LEMMA_KEY, |lemma| {
                let maybe_vocab_idx = vocab.get(lemma);
                if let Some(vocab_idx) = maybe_vocab_idx {
                    *counts.entry(*vocab_idx).or_insert(0) += 1;
                    doc_words += 1;
                }
            });
            sender.send(Some((doc_words, counts))).unwrap();
        }
    );
    sender.send(None).unwrap();
    pipe_reader.join().unwrap()
    */
    let timer = howlong::ProcessCPUTimer::new();
    //let pool = mk_default_pool();
    //let pool2 = mk_default_pool();
    //let mut word_freqs_strings = pool.scope(|scope| {
        //pool2.install(|| {
    let mut word_freqs_strings = iter_flat_tokens(xml_entries, mmap, LEMMA_KEY)
        .fold(
            || {
                return BTreeMap::<Box<[u8]>, u32>::new();
            },
            |mut acc, elem| {
                *acc.entry(elem).or_insert(0) += 1;
                return acc;
            }
        ).reduce(
            || {
                return BTreeMap::<Box<[u8]>, u32>::new();
            },
            |left, right| {
                let (mut acc, rest) = if left.len() < right.len() {
                    (right, left)
                } else {
                    (left, right)
                };
                for (elem, cnt) in rest.into_iter() {
                    *acc.entry(elem).or_insert(0) += cnt;
                }
                return acc;
            }
        ).into_iter().collect_vec();
        //})
    //});
    println!("Gather counts {}", timer.elapsed());
    let timer = howlong::ProcessCPUTimer::new();
    word_freqs_strings
        .sort_unstable_by(
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
    println!("Sort and reindex {}", timer.elapsed());
    (vocab, word_freqs_indexed, total_words)
}

fn main() {
    let args: MkDisp = argh::from_env();

    if args.input.len() == 0 {
        panic!("Need at least one input")
    } else if args.input.len() > 1 {
        panic!("Multiple inputs not supported yet")
    }

    println!("Processing using {} threads", current_num_threads());
    let timer = howlong::ProcessCPUTimer::new();
    let mmap = mmap_file(Path::new(&args.input[0]));
    let zip_reader: ZipArchive = ZipArchive::new(&mmap).unwrap();
    let xml_entries = filter_xml_entries(&zip_reader);
    println!("Read EOCDR {}", timer.elapsed());

    let num_docs = xml_entries.len() as u32;
    let (vocab, word_counts, total_words) = one_scan_index_count(&xml_entries, &mmap);

    let timer = howlong::ProcessCPUTimer::new();
    //let pool = mk_default_pool();
    //let pool2 = mk_default_pool();
    //let kl_divs = pool.scope(|scope| {
        //pool2.install(|| {
    let word_accs = iter_doc_bows(&xml_entries, &mmap, &vocab, LEMMA_KEY)
        .fold(
            || {
                return BTreeMap::<u32, AccElement>::new();
            },
            |mut acc, (doc_words_total, doc_word_counts)| {
                for (elem, cnt) in doc_word_counts.into_iter() {
                    let left = acc.entry(elem).or_insert(AccElement::zero());
                    let word_count = word_counts[elem as usize];
                    *left = reduce_word(left, &acc_word(cnt, word_count, doc_words_total, total_words, num_docs));
                }
                return acc;
            }
        ).reduce(
            || {
                return BTreeMap::<u32, AccElement>::new();
            },
            |left, right| {
                let (mut acc, rest) = if left.len() < right.len() {
                    (right, left)
                } else {
                    (left, right)
                };
                for (elem, div) in rest.into_iter() {
                    let left = acc.entry(elem).or_insert(AccElement::zero());
                    *left = reduce_word(left, &div);
                }
                acc
            }
        );
        //})
    //});
    let mut cols = FinalColumns::with_capacity(total_words as usize);
    word_accs.into_iter().for_each(|(word_id, elem)| {
        norm_word(&mut cols, elem, word_counts[word_id as usize], total_words, num_docs)
    });
    println!("Gather KL divergences {}", timer.elapsed());
    let timer = howlong::ProcessCPUTimer::new();
    let (mut words, index): (Vec<Box<[u8]>>, Vec<u32>) = vocab.into_iter().unzip();
    let mut index_islice = index.into_iter().map(|x| x as isize).collect_vec();
    words.as_mut_slice().apply_inverse_permutation(index_islice.as_mut_slice());
    println!("Postprocessing of KL divergences {}", timer.elapsed());
    let timer = howlong::ProcessCPUTimer::new();
    write_parquet(
        Path::new(&args.output),
        words.as_slice(),
        word_counts.as_slice(),
        &[
            "kl_div",
            "idf",
        ],
        &[
            cols.kl_div.as_slice(),
            cols.idf.as_slice(),
        ]
    );
    println!("Writing to parquet file {}", timer.elapsed());
}
