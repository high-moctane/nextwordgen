use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

const READ_LINES_COUNT: i32 = 100;

pub fn run() {
    let src = Path::new("short.txt.gz");
    let dstdir = Path::new("dst");
    fs::create_dir(dstdir).unwrap();

    read_raw_data(&src, &dstdir);

    println!("done.");

    merge_dir(&dstdir);
}

/// Reads raw data and dump into multiple files.
fn read_raw_data(src: &Path, dstdir: &Path) {
    let src_f = File::open(src).unwrap();
    let src_f = BufReader::new(src_f);
    let src_f = GzDecoder::new(src_f);
    let mut lines = BufReader::new(src_f).lines();

    let mut file_cnt = 0;
    'read_loop: loop {
        let dstfile = dstdir.to_path_buf().join(&format!("{}.gz", file_cnt));
        let dstfile = File::create(dstfile).unwrap();
        let dstfile = GzEncoder::new(dstfile, Compression::default());
        let mut dstfile = BufWriter::new(dstfile);

        let mut counter = EntryCounter::new();

        for _ in 0..READ_LINES_COUNT {
            match lines.next() {
                Some(result) => {
                    counter.add_from_str(&result.unwrap());
                }
                None => {
                    counter.dump(&mut dstfile).unwrap();
                    dstfile.flush().unwrap();
                    break 'read_loop;
                }
            }
        }

        counter.dump(&mut dstfile).unwrap();
        dstfile.flush().unwrap();
        file_cnt += 1;
    }
}

/// merge two files into dst.
/// FIXME
fn merge_files(src1: &Path, src2: &Path, dst: &Path) {
    let src1_f = File::open(src1).unwrap();
    let src1_f = BufReader::new(src1_f);
    let src1_f = GzDecoder::new(src1_f);
    let mut lines1 = BufReader::new(src1_f).lines();

    let src2_f = File::open(src2).unwrap();
    let src2_f = BufReader::new(src2_f);
    let src2_f = GzDecoder::new(src2_f);
    let mut lines2 = BufReader::new(src2_f).lines();

    let dst_f = File::create(dst).unwrap();
    let dst_f = GzEncoder::new(dst_f, Compression::default());
    let mut dst_f = BufWriter::new(dst_f);

    let mut update_entry1 = || match lines1.next() {
        Some(result) => match result {
            Ok(s) => Entry::new(&s),
            Err(err) => panic!(err),
        },
        None => None,
    };
    let mut update_entry2 = || match lines2.next() {
        Some(result) => match result {
            Ok(s) => Entry::new(&s),
            Err(err) => panic!(err),
        },
        None => None,
    };

    let mut entry1 = update_entry1();
    let mut entry2 = update_entry2();

    loop {
        if entry1.is_none() && entry2.is_none() {
            dst_f.flush().unwrap();
            return;
        } else if entry2.is_none() {
            writeln!(dst_f, "{}", entry1.unwrap()).unwrap();
            entry1 = update_entry1();
        } else if entry1.is_none() {
            writeln!(dst_f, "{}", entry2.unwrap()).unwrap();
            entry2 = update_entry1();
        } else if &entry1.unwrap().ngram < entry2.unwrap().ngram {
            writeln!(dst_f, "{}", entry1.unwrap()).unwrap();
            entry1 = update_entry1();
        }
    }
}

fn merge_dir(dir: &Path) {
    loop {
        thread::sleep(Duration::from_secs(1));

        let mut all_files: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|result| result.unwrap().path())
            .collect();

        all_files.sort_by(|a, b| file_name_num(b).cmp(&file_name_num(a)));

        if all_files.len() < 2 {
            return;
        }

        for two_files in all_files.chunks(2) {
            if two_files.len() < 2 {
                break;
            }

            let file_num = (file_name_num(&two_files[0]) + 1).to_string();
            let mut file_name = PathBuf::from(two_files[0].parent().unwrap());
            file_name.push(&format!("{}.gz", file_num));
            merge_files(&two_files[0], &two_files[1], &Path::new(&file_name));

            fs::remove_file(&two_files[0]).unwrap();
            fs::remove_file(&two_files[1]).unwrap();
        }
    }
}

fn file_name_num(path: &Path) -> i32 {
    path.file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<i32>()
        .unwrap()
}

/// Entry has a pair of n-gram wors and match count.
#[derive(Debug, PartialEq)]
struct Entry {
    ngram: String,
    match_count: u128,
}

impl Entry {
    /// Parses s (n-gram line) into Entry.
    fn new(s: &str) -> Option<Entry> {
        let mut s_iter = s.split("\t");

        let mut ngram = vec![];
        for word in s_iter.next().unwrap().split(" ") {
            match Entry::valid_ngram_elem(word) {
                Some(w) => ngram.push(w),
                None => return None,
            }
        }
        let ngram = ngram.join(" ");

        s_iter.next(); // year
        let match_count = s_iter.next().unwrap().parse::<u128>().unwrap();

        Some(Entry { ngram, match_count })
    }

    /// Judges whether the word is valid or not.
    fn valid_ngram_elem(word: &str) -> Option<String> {
        match word.starts_with("_") {
            true => None,
            false => Some(word.split("_").next().unwrap().to_string()),
        }
    }

    fn from_parsed_str(s: &str) -> Entry {
        let elems: Vec<&str> = s.split("\t").collect();
        let match_count = elems[1].parse::<u128>().unwrap();
        Entry {
            ngram: elems[0].to_string(),
            match_count,
        }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\t{}", self.ngram, self.match_count)
    }
}

/// EntryCounter counts frequency of n-grams.
#[derive(Debug)]
struct EntryCounter {
    data: BTreeMap<String, u128>,
}

impl EntryCounter {
    /// Makes new EntryCounter instance.
    fn new() -> EntryCounter {
        EntryCounter {
            data: BTreeMap::new(),
        }
    }

    /// Adds entry into EntryCounter.
    fn add(&mut self, entry: Entry) {
        self.data
            .entry(entry.ngram.clone())
            .and_modify(|cnt| *cnt += entry.match_count)
            .or_insert(entry.match_count);
    }

    /// Adds entry from raw str.
    fn add_from_str(&mut self, s: &str) {
        match Entry::new(s) {
            Some(entry) => self.add(entry),
            None => return,
        }
    }

    /// Dumps self.data into w.
    fn dump(&self, w: &mut io::Write) -> io::Result<()> {
        for (ngram, match_count) in &self.data {
            writeln!(w, "{}\t{}", ngram, match_count)?;
        }
        w.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_ngram_elem() {
        assert_eq!(Entry::valid_ngram_elem("text"), Some("text".to_string()));
        assert_eq!(
            Entry::valid_ngram_elem("text_NOUN"),
            Some("text".to_string())
        );
        assert_eq!(Entry::valid_ngram_elem("_NOUN_"), None);
    }

    #[test]
    fn test_entry_new() {
        let inputs = vec!["a\t2018\t10\t20\n", "a b c\t2018\t10\t20\n"];
        let results = vec![
            Some(Entry {
                ngram: "a".to_string(),
                match_count: 10,
            }),
            Some(Entry {
                ngram: "a b c".to_string(),
                match_count: 10,
            }),
        ];

        for (input, result) in inputs.into_iter().zip(results) {
            assert_eq!(Entry::new(input), result);
        }
    }

    #[test]
    fn test_entry_counter() {
        let inputs = vec![
            "d e f\t2016\t10\t20",
            "a b c\t2018\t10\t20",
            "g h i\t2018\t90\t20",
            "d e f\t2017\t20\t20",
            "d e f\t2018\t30\t20",
        ];
        let results: Vec<(&str, u128)> = vec![("a b c", 10), ("d e f", 60), ("g h i", 90)];

        let mut counter = EntryCounter::new();

        for input in inputs {
            counter.add_from_str(&input);
        }

        for (entry, result) in counter.data.iter().zip(results) {
            assert_eq!(*entry.0, result.0);
            assert_eq!(*entry.1, result.1);
        }
    }
}
