use std::path::Path;
use wordfreak::types::Corpus;
use wordfreak::vocab::VocabMap;
use argh::FromArgs;
use std::collections::BTreeMap;
use itertools::Itertools;
use superslice::*;
use wordfreak::parquet2::write_parquet;
use wordfreak::dispersion::{AccElement, acc_word, reduce_word, norm_word, FinalColumns};
use wordfreak::corpus::{CorpusType, get_corpus};


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

/// Indexes the collection and at the same time collects counts per word, as well as the total
/// token count.
fn one_scan_index_count(corpus: &Box<dyn Corpus>) -> (VocabMap, Vec<u32>, u32, u32) {
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
    let (vocab_builder, doc_count) = corpus.count_words();
    let (vocab, word_freqs_indexed, total_words) = vocab_builder.build();
    println!("Gather counts {}", timer.elapsed());
    let timer = howlong::ProcessCPUTimer::new();
    println!("Sort and reindex {}", timer.elapsed());
    (vocab, word_freqs_indexed, total_words, doc_count)
}

fn process_corpus(corpus: &Box<dyn Corpus>, output: &str) {
    let (vocab, word_counts, total_words, num_docs) = one_scan_index_count(corpus);

    let timer = howlong::ProcessCPUTimer::new();
    let word_accs = crossbeam::scope(|scope| {
        let rcv = corpus.gen_doc_bows(scope, &vocab);
        let mut acc = BTreeMap::<u32, AccElement>::new();
        for (doc_words_total, doc_word_counts) in rcv.into_iter() {
            for (elem, cnt) in doc_word_counts.into_iter() {
                let left = acc.entry(elem).or_insert(AccElement::zero());
                let word_count = word_counts[elem as usize];
                *left = reduce_word(left, &acc_word(cnt, word_count, doc_words_total, total_words, num_docs));
            }
            /*let left = acc.entry(elem).or_insert(AccElement::zero());
            *left = reduce_word(left, &div);*/
        }
        acc
    }).unwrap();
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
        Path::new(output),
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

fn main() {
    let args: MkDisp = argh::from_env();

    if args.input.len() == 0 {
        panic!("Need at least one input")
    } else if args.input.len() > 1 {
        panic!("Multiple inputs not supported yet")
    }

    let corpus_path = Path::new(&args.input[0]);
    let corpus = get_corpus(corpus_path, args.corpus_type.unwrap());
    process_corpus(&corpus, &args.output)
}
