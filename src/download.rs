use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::fs::DirEntry;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time;

// const READ_LINES_NUM: u32 = 1000000000;
const READ_LINES_NUM: u32 = 100000;

pub fn run() {
    count_data(&Path::new("2gram-wy.txt.gz"), &Path::new("dstdir")).unwrap();
    merge_files(Path::new(&Path::new("dstdir"))).unwrap();
}

/// open *.gz file.
fn read_gz(path: &Path) -> io::Result<BufReader<GzDecoder<BufReader<File>>>> {
    let f = File::open(path)?;
    let r = BufReader::new(f);
    let decoder = GzDecoder::new(r);
    Ok(BufReader::new(decoder))
}

/// create *.gz file.
fn write_gz(path: &Path) -> io::Result<BufWriter<GzEncoder<BufWriter<File>>>> {
    let f = File::create(path)?;
    let w = BufWriter::new(f);
    let encoder = GzEncoder::new(w, Compression::best());
    Ok(BufWriter::new(encoder))
}

/// count data from raw gzip file to multiple gzip files.
fn count_data(src: &Path, dst_dir: &Path) -> io::Result<()> {
    let r = read_gz(src)?;

    let mut entry_counter = EntryCounter::new();

    let mut file_num = 0u32;

    let mut lines = r.lines();

    loop {
        let mut dst_path = dst_dir.to_path_buf();
        dst_path.push(&format!("{:010}.txt.gz", file_num));

        let mut w = write_gz(&dst_path)?;

        for _ in 0..READ_LINES_NUM {
            match lines.next() {
                Some(result) => match result {
                    Ok(line) => match Entry::from_raw_line(&line) {
                        Some(entry) => entry_counter.add(&entry),
                        None => continue,
                    },
                    Err(err) => return Err(err),
                },
                None => {
                    entry_counter.dump_to(&mut w)?;
                    return Ok(());
                }
            }
        }

        entry_counter.dump_to(&mut w)?;

        file_num += 1;
    }
}

fn merge_two_files(src1: &Path, src2: &Path, dst: &Path) -> io::Result<()> {
    let r1 = read_gz(src1)?;
    let r2 = read_gz(src2)?;

    let mut lines1 = r1.lines();
    let mut lines2 = r2.lines();

    let mut w = write_gz(dst)?;

    // fetches new entry
    let mut update1 = move || -> io::Result<Option<Entry>> {
        match lines1.next() {
            Some(result) => match result {
                Ok(line) => Ok(Some(Entry::from_parsed_line(&line))),
                Err(err) => Err(err),
            },
            None => Ok(None),
        }
    };
    let mut update2 = move || -> io::Result<Option<Entry>> {
        match lines2.next() {
            Some(result) => match result {
                Ok(line) => Ok(Some(Entry::from_parsed_line(&line))),
                Err(err) => Err(err),
            },
            None => Ok(None),
        }
    };

    let mut entry1 = update1()?;
    let mut entry2 = update2()?;

    loop {
        // done it
        if entry1.is_none() && entry2.is_none() {
            return Ok(());
        }

        // only entry1 is available
        if entry2.is_none() {
            entry1.unwrap().dump_to(&mut w)?;
            entry1 = update1()?;
            continue;
        }

        // only entry2 is available
        if entry1.is_none() {
            entry2.unwrap().dump_to(&mut w)?;
            entry2 = update2()?;
            continue;
        }

        // compare the ordering of the two entries
        let ent1 = entry1.as_ref().unwrap();
        let ent2 = entry2.as_ref().unwrap();
        match ent1.entry_cmp(&ent2) {
            EntryOrd::Less => {
                ent1.dump_to(&mut w)?;
                entry1 = update1()?;
            }
            EntryOrd::Equal => {
                ent1.merge(&ent2).dump_to(&mut w)?;
                entry1 = update1()?;
                entry2 = update2()?;
            }
            EntryOrd::Grater => {
                ent2.dump_to(&mut w)?;
                entry2 = update2()?;
            }
        }
    }
}

/// merge file(s) into one file.
fn merge_files(dir: &Path) -> io::Result<()> {
    let mut dir_entries = fs::read_dir(dir)?.collect::<Vec<io::Result<DirEntry>>>();

    // when the file number is one, the loop is over.
    if dir_entries.len() < 2 {
        return Ok(());
    }

    dir_entries.reverse();

    for src in dir_entries.chunks(2) {
        if src.len() < 2 {
            break;
        }

        // TODO: remove unwrap()
        let src1 = src[0].as_ref().unwrap().path();
        let src2 = src[1].as_ref().unwrap().path();

        // .DS_Store!!!!!!!!!!!!!(´･ω･｀)
        if !is_valid_gzip_file_name(&src1) || !is_valid_gzip_file_name(&src2) {
            break;
        }

        let new_number = src1
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(".")
            .next()
            .unwrap()
            .parse::<u32>()
            .unwrap()
            + 1;
        let mut dst = src1.parent().unwrap().to_path_buf();
        dst.push(&format!("{:010}.txt.gz", new_number));

        merge_two_files(&src1, &src2, &dst)?;

        fs::remove_file(&src1)?;
        fs::remove_file(&src2)?;
    }

    // recursion
    // short sleep is needed
    thread::sleep(time::Duration::from_millis(100));
    merge_files(dir)
}

/// .DS_Store!!!!!!!!!!!(´･ω･｀)
fn is_valid_gzip_file_name(path: &Path) -> bool {
    match path.extension() {
        Some(os_str) => os_str.to_str().unwrap() == "gz",
        None => false,
    }
}

type Ngram = Vec<String>;
type Count = u128;

/// Entry is a struct of each line of data.
#[derive(Debug, PartialEq)]
struct Entry {
    ngram: Ngram,
    match_count: Count,
}

/// EntryOrd is the result of a comparison between two entries.
#[derive(Debug, PartialEq)]
enum EntryOrd {
    Less,
    Equal,
    Grater,
}

impl Entry {
    /// Parses from raw line.
    fn from_raw_line(line: &str) -> Option<Entry> {
        let elems: Vec<&str> = line.split("\t").collect();

        if elems.len() != 4 {
            return None;
        }

        let ngram = Entry::split_ngram_to_words(elems[0]);
        if ngram.is_none() {
            return None;
        }
        let ngram = ngram.unwrap();

        let match_count = elems[2].parse::<Count>();
        if match_count.is_err() {
            return None;
        }
        let match_count = match_count.unwrap();

        Some(Entry { ngram, match_count })
    }

    /// Splits s into valid words.
    fn split_ngram_to_words(s: &str) -> Option<Ngram> {
        let opt_words: Vec<Option<String>> = s
            .split(" ")
            .map(|word| Entry::valid_ngram_elem(word))
            .collect();

        if opt_words.contains(&None) {
            None
        } else {
            Some(opt_words.into_iter().map(|opt| opt.unwrap()).collect())
        }
    }

    /// Extracts valid word.
    fn valid_ngram_elem(word: &str) -> Option<String> {
        if word.starts_with("_") {
            None
        } else {
            Some(word.split("_").next().unwrap().to_string())
        }
    }

    /// Parses from sanity line.
    fn from_parsed_line(line: &str) -> Entry {
        let mut elems_iter = line.split("\t");

        let ngram: Ngram = elems_iter
            .next()
            .unwrap()
            .split(" ")
            .map(|word| word.to_string())
            .collect();

        let match_count = elems_iter.next().unwrap().parse::<Count>().unwrap();

        Entry { ngram, match_count }
    }

    /// entry_cmp returns the original result of comparison.
    fn entry_cmp(&self, other: &Entry) -> EntryOrd {
        if self.ngram < other.ngram {
            EntryOrd::Less
        } else if self.ngram == other.ngram {
            EntryOrd::Equal
        } else {
            EntryOrd::Grater
        }
    }

    /// merge merges two emtries which have the same entry.
    fn merge(&self, other: &Entry) -> Entry {
        Entry {
            ngram: self.ngram.clone(),
            match_count: self.match_count + other.match_count,
        }
    }

    /// dump_to dumps entry to w.
    fn dump_to(&self, w: &mut Write) -> io::Result<()> {
        writeln!(w, "{}", self)
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\t{}", self.ngram.join(" "), self.match_count)
    }
}

/// EntryCounter counts frequency of n-gram entry.
struct EntryCounter {
    data: BTreeMap<Ngram, Count>,
}

impl EntryCounter {
    fn new() -> EntryCounter {
        EntryCounter {
            data: BTreeMap::new(),
        }
    }

    /// Adds entry into EntryCounter
    fn add(&mut self, entry: &Entry) {
        self.data
            .entry(entry.ngram.clone())
            .and_modify(|cnt| *cnt += entry.match_count)
            .or_insert(entry.match_count);
    }

    /// dump_to dump data into w.
    fn dump_to(&self, w: &mut io::Write) -> io::Result<()> {
        for (ngram, match_count) in &self.data {
            let entry = Entry {
                ngram: ngram.to_vec(),
                match_count: *match_count,
            };
            entry.dump_to(w)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test_entry {
    use super::*;

    #[test]
    fn from_raw_line() {
        let query = vec![
            "a b\t2019\t10\t1",
            "c_NOUN d\t2019\t20\t1",
            "e_NOUN _f_\t2019\t30\t1",
            "あ い\t2019\t10\t1",
        ];
        let ans = vec![
            Some(Entry {
                ngram: vec!["a".to_string(), "b".to_string()],
                match_count: 10,
            }),
            Some(Entry {
                ngram: vec!["c".to_string(), "d".to_string()],
                match_count: 20,
            }),
            None,
            None,
        ];

        for (q, a) in query.into_iter().zip(ans.into_iter()) {
            assert_eq!(Entry::from_raw_line(q), a);
        }
    }

    #[test]
    fn from_parsed_line() {
        assert_eq!(
            Entry::from_parsed_line("a b\t1"),
            Entry {
                ngram: vec!["a".to_string(), "b".to_string()],
                match_count: 1
            }
        )
    }

    #[test]
    fn fmt() {
        assert_eq!(format!("{}", Entry::from_parsed_line("a b\t1")), "a b\t1")
    }

    #[test]
    fn entry_cmp() {
        let entry = Entry::from_parsed_line("b b\t1");
        let others = vec![
            Entry::from_parsed_line("b c\t6"),
            Entry::from_parsed_line("b b\t5"),
            Entry::from_parsed_line("a b\t4"),
        ];
        let answer = vec![EntryOrd::Less, EntryOrd::Equal, EntryOrd::Grater];

        for (other, ans) in others.into_iter().zip(answer.into_iter()) {
            assert_eq!(entry.entry_cmp(&other), ans);
        }
    }
}

#[cfg(test)]
mod test_entry_counter {
    use super::*;

    #[test]
    fn add() {
        let entries = vec![
            Entry::from_parsed_line("c a\t1"),
            Entry::from_parsed_line("a b\t2"),
            Entry::from_parsed_line("b c\t3"),
            Entry::from_parsed_line("a c\t4"),
            Entry::from_parsed_line("a b\t5"),
        ];
        let answer = vec![
            Entry::from_parsed_line("a b\t7"),
            Entry::from_parsed_line("a c\t4"),
            Entry::from_parsed_line("b c\t3"),
            Entry::from_parsed_line("c a\t1"),
        ];

        let mut counter = EntryCounter::new();

        for ent in &entries {
            counter.add(ent);
        }

        let mut ret = vec![];
        for (ngram, match_count) in counter.data.into_iter() {
            let entry = Entry { ngram, match_count };
            ret.push(entry);
        }

        assert_eq!(ret, answer);
    }
}
