use std::io::BufReader;
use std::io;

use piz::ZipArchive;
use quick_xml::events::Event;
use rayon::prelude::*;
use piz::read::FileMetadata;
use std::ffi::OsStr;
use internal_iterator::InternalIterator;

// Should probably be bigger than normal because deflate adds latency(?)
const READ_CHUNK_SIZE: usize = 64 * 1024;


fn is_vrt_file(entry: &FileMetadata) -> bool {
    entry.is_file() && entry.path.extension().and_then(OsStr::to_str) == Some("vrt")
}


pub fn iter_readers<'a, 'b>(zip_reader: &'a ZipArchive<'b>) -> impl ParallelIterator<Item=quick_xml::Reader<BufReader<Box<dyn std::io::Read + Send + 'b>>>> + 'a {
    zip_reader.entries().par_iter().filter_map(move |entry| {
        if is_vrt_file(entry) {
            Some(quick_xml::Reader::from_reader(
                BufReader::with_capacity(READ_CHUNK_SIZE, zip_reader.read(entry).unwrap())
            ))
        } else {
            None
        }
    })
}

struct VrtFile<'a, 'b, F> {
    buf: Vec::<u8>,
    reader: &'b mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
    proc_doc: F
}

impl<'a, 'b, F> VrtFile<'a, 'b, F> {
    fn new(
        reader: &'b mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
        proc_doc: F
    ) -> VrtFile<'a, 'b, F> {
        VrtFile { buf: Vec::new(), reader, proc_doc }
    }

    fn new_with_buf(
        buf: Vec::<u8>,
        reader: &'b mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
        proc_doc: F
    ) -> VrtFile<'a, 'b, F> {
        VrtFile { buf, reader, proc_doc }
    }
}

impl<'a, 'b, R, F: FnMut(VrtText) -> Option<R>> Iterator for VrtFile<'a, 'b, F> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_event(&mut self.buf) {
                Ok(Event::Start(ref e)) => {
                    match e.name() {
                        b"text" => {
                            return (self.proc_doc)(VrtText {
                                reader: self.reader,
                                buf: &mut self.buf,
                            });
                        },
                        _ => {}
                    }
                },
                Ok(Event::Eof) => return None,
                Err(e) => panic!("Error at position {}: {:?}", self.reader.buffer_position(), e),
                _ => (),
            }
            self.buf.clear();
        }
    }
}

struct VrtText<'a, 'b> {
    reader: &'a mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'b>>>,
    buf: &'a mut Vec::<u8>
}

impl<'a, 'b> InternalIterator for VrtText<'a, 'b> {
    type Item = &'a [u8];

    fn find_map<T, F>(self, mut f: F) -> Option<T>
    where
        F: FnMut(&'a [u8]) -> Option<T>
    {
        let mut in_sent = false;
        loop {
            match self.reader.read_event(self.buf) {
                Ok(Event::Start(ref e)) => {
                    match e.name() {
                        b"sentence" => {
                            in_sent = true;
                        },
                        _ => {}
                    }
                },
                Ok(Event::End(ref e)) => {
                    match e.name() {
                        b"text" => {
                            return None;
                        },
                        b"sentence" => {
                            in_sent = false
                        },
                        _ => {}
                    }
                },
                Ok(Event::Text(ref e)) => {
                    if in_sent {
                        println!("{:?}", e);
                        let res = f(b"hello");
                        if res.is_some() {
                            return res
                        }
                    }
                },
                Ok(Event::Eof) => panic!("Premature end! Ended inside <text>."),
                Err(e) => panic!("Error at position {}: {:?}", self.reader.buffer_position(), e),
                _ => (),
            }
            self.buf.clear();
        }
    }
}

pub fn iter_flat_tokens<'a>(zip_reader: &'a ZipArchive<'a>, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=Box<[u8]>> + 'a {
    iter_readers(zip_reader)
        .flat_map_iter(move |mut reader| {
            let mut res = vec!();
            VrtFile::new(&mut reader, |vrt_text: VrtText| -> Option<()> {
                vrt_text.for_each(|tok| {
                    res.push(tok.to_owned().into_boxed_slice());
                });
                None
            });
            res.into_iter()
        })
}
