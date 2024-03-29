use bzip2::bufread::BzDecoder;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use clap::Parser;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const BATCH_SIZE: u64 = 100;
const PROGRESS_COUNT: u64 = 100000;

#[macro_use]
extern crate lazy_static_include;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(long)]
    labels: bool,
    #[clap(long)]
    statement_counts: bool,
    #[clap(short, long, default_value = "0")]
    skip: u64,
    #[clap(short, long)]
    threads: Option<usize>,
    #[clap(required = true)]
    paths: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Extra<'a> {
    None,
    Type(&'a str),
    Lang(&'a str),
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Subject<'a> {
    IRI(&'a str),
    Blank(&'a str),
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Object<'a> {
    IRI(&'a str),
    Blank(&'a str),
    Literal(&'a str, Extra<'a>),
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Statement<'a> {
    subject: Subject<'a>,
    predicate: &'a str,
    object: Object<'a>,
}

pub enum Work {
    LINES(u64, Vec<String>),
    DONE,
}

pub struct WorkResult {
    statement_counts: Option<HashMap<String, u64>>,
}

lazy_static! {
    static ref RE: Regex = Regex::new(
        r#"(?x)
        ^
        \s*

        # subject

        (?:

          # IRI
          (?:<([^>]*)>)

          |

          # Blank

          (?:_:([^\s]+))
        )

        \s*

        # predicate IRI
        <([^>]*)>

        \s*

        # object
        (?:

          # IRI
          (?:<([^>]*)>)

          |

          # Blank

          (?:_:([^\s]+))

          |

          # literal
          (?:

            "([^"]*)"

            # optional extra
            (?:

              # language
              (?:@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*))

              |

              # data type
              (?:\^\^<([^>]*)>)
            )?
          )
        )
        "#
    )
    .unwrap();
}

pub fn parse<'a>(line: u64, input: &'a str, regex: &Regex) -> Statement<'a> {
    let captures = regex
        .captures(input)
        .unwrap_or_else(|| panic!("Invalid line: {}: {:?}", line, input));

    let subject = captures
        .get(1)
        .map(|object| Subject::IRI(object.as_str()))
        .or_else(|| captures.get(2).map(|blank| Subject::Blank(blank.as_str())))
        .expect("failed to parse subject");

    let predicate = captures.get(3).expect("failed to parse predicate").as_str();

    let object = captures
        .get(4)
        .map(|object| Object::IRI(object.as_str()))
        .or_else(|| captures.get(5).map(|blank| Object::Blank(blank.as_str())))
        .unwrap_or_else(|| {
            let literal = captures.get(6).expect("failed to parse object").as_str();
            let extra = captures
                .get(7)
                .map(|lang| Extra::Lang(lang.as_str()))
                .or_else(|| {
                    captures
                        .get(8)
                        .map(|data_type| Extra::Type(data_type.as_str()))
                })
                .unwrap_or(Extra::None);
            Object::Literal(literal, extra)
        });
    Statement {
        subject,
        predicate,
        object,
    }
}

lazy_static_include_str! {
    PROPERTIES_DATA => "properties",
    IDENTIFIER_PROPERTIES_DATA => "identifier-properties",
    LANGUAGES_DATA => "languages",
    LABELS_DATA => "labels",
}

lazy_static! {
    static ref PROPERTIES: HashSet<&'static str> = line_set(&PROPERTIES_DATA);
}

lazy_static! {
    static ref IDENTIFIER_PROPERTIES: HashSet<String> = line_set(&IDENTIFIER_PROPERTIES_DATA)
        .iter()
        .flat_map(|id| vec![
            format!("http://www.wikidata.org/prop/direct/P{}", id),
            format!("http://www.wikidata.org/prop/direct-normalized/P{}", id)
        ])
        .collect();
}

lazy_static! {
    static ref LANGUAGES: HashSet<&'static str> = line_set(&LANGUAGES_DATA);
}

lazy_static! {
    static ref LABELS: HashSet<&'static str> = line_set(&LABELS_DATA);
}

fn line_set(data: &str) -> HashSet<&str> {
    data.lines().collect()
}

fn ignored_subject(iri: &str) -> bool {
    iri.starts_with("https://www.wikidata.org/wiki/Special:EntityData")
}

fn produce<T: Read>(
    running: Arc<AtomicBool>,
    skip: u64,
    reader: T,
    s: &Sender<Work>,
) -> (bool, u64) {
    let mut total = 0;
    let mut buf_reader = BufReader::new(reader);

    let mut lines = Vec::new();

    if skip > 0 {
        eprintln!("# skipping {}", skip)
    }

    loop {
        if !running.load(Ordering::SeqCst) {
            eprintln!("# interrupted after {}", total);
            return (false, total);
        }

        let mut line = String::new();
        if buf_reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        total += 1;

        let skipped = total < skip;

        if !skipped {
            lines.push(line);

            if total % BATCH_SIZE == 0 {
                s.send(Work::LINES(total, lines)).unwrap();
                lines = Vec::new();
            }
        }

        if total % PROGRESS_COUNT == 0 {
            let status = if skipped { "skipped" } else { "" };
            eprintln!("# {} {}", status, total);
        }
    }

    if !lines.is_empty() {
        s.send(Work::LINES(total, lines)).unwrap();
    }

    (true, total)
}

fn consume(
    name: String,
    work_receiver: Receiver<Work>,
    result_sender: Sender<WorkResult>,
    labels: bool,
    statement_counts: bool,
) {
    let regex = RE.clone();

    let lines_path = format!("{}.nt.bz2", name);
    let lines_file = File::create(&lines_path)
        .unwrap_or_else(|_| panic!("unable to create file: {}", &lines_path));
    let mut lines_encoder = BzEncoder::new(BufWriter::new(lines_file), Compression::best());

    let mut labels_encoder = if labels {
        let labels_path = format!("labels_{}.bz2", name);
        let labels_file = File::create(&labels_path)
            .unwrap_or_else(|_| panic!("unable to create file: {}", &labels_path));
        Some(BzEncoder::new(
            BufWriter::new(labels_file),
            Compression::best(),
        ))
    } else {
        None
    };

    let mut statement_counter = if statement_counts {
        Some(HashMap::new())
    } else {
        None
    };

    loop {
        match work_receiver.recv().unwrap() {
            Work::LINES(number, lines) => {
                for line in lines {
                    handle(
                        &mut lines_encoder,
                        labels_encoder.as_mut(),
                        statement_counter.as_mut(),
                        number,
                        line,
                        &regex,
                    );
                }
                lines_encoder.flush().unwrap();
                if let Some(labels_encoder) = labels_encoder.as_mut() {
                    labels_encoder.flush().unwrap()
                }
            }
            Work::DONE => {
                eprintln!("# stopping thread {}", name);
                lines_encoder.try_finish().unwrap();
                if let Some(labels_encoder) = labels_encoder.as_mut() {
                    labels_encoder.try_finish().unwrap()
                }

                result_sender
                    .send(WorkResult {
                        statement_counts: statement_counter,
                    })
                    .unwrap();

                return;
            }
        }
    }
}

fn handle<T: Write, U: Write>(
    lines_writer: &mut T,
    labels_writer: Option<&mut U>,
    statement_counter: Option<&mut HashMap<String, u64>>,
    number: u64,
    line: String,
    regex: &Regex,
) -> Option<()> {
    let statement = parse(number, &line, regex);
    maybe_write_line(lines_writer, &line, statement);
    let id = entity(statement.subject)?;
    maybe_count_statement(statement_counter, id, statement);
    maybe_write_label(labels_writer, id, statement);
    None
}

fn maybe_write_line<T: Write>(lines_writer: &mut T, line: &str, statement: Statement) {
    if !is_acceptable(statement) {
        return;
    }

    lines_writer.write_all(line.as_bytes()).unwrap();
}

fn maybe_write_label<T: Write>(
    labels_writer: Option<&mut T>,
    id: &str,
    statement: Statement,
) -> Option<()> {
    let labels_writer = labels_writer?;
    let label = label(statement)?;
    labels_writer
        .write_fmt(format_args!("{} {}\n", id, label))
        .unwrap();
    None
}

fn maybe_count_statement(
    statement_counter: Option<&mut HashMap<String, u64>>,
    id: &str,
    statement: Statement,
) -> Option<()> {
    let statement_counter = statement_counter?;
    direct_property(statement.predicate)?;
    *statement_counter.entry(id.to_string()).or_insert(0) += 1;
    None
}

fn is_acceptable(statement: Statement) -> bool {
    if PROPERTIES.contains(statement.predicate)
        || IDENTIFIER_PROPERTIES.contains(statement.predicate)
    {
        return false;
    }
    match statement.subject {
        Subject::Blank(_) => return false,
        Subject::IRI(iri) if ignored_subject(iri) => return false,
        _ => (),
    }
    match statement.object {
        Object::Blank(_) => return false,
        Object::Literal(_, Extra::Lang(lang)) if !LANGUAGES.contains(lang) => return false,
        // non-Earth geo coordinates are not supported by some triple stores
        Object::Literal(
            literal,
            Extra::Type("http://www.opengis.net/ont/geosparql#wktLiteral"),
        ) if literal.starts_with('<') => return false,
        _ => (),
    }

    true
}

fn label(statement: Statement) -> Option<String> {
    if !LABELS.contains(statement.predicate) {
        return None;
    }

    if let Object::Literal(label, Extra::Lang(lang)) = statement.object {
        if !LANGUAGES.contains(lang) {
            return None;
        }

        return Some(unescape(label));
    }

    None
}
static ENTITY_IRI_PREFIX: &str = "http://www.wikidata.org/entity/Q";

fn entity(subject: Subject) -> Option<&str> {
    if let Subject::IRI(iri) = subject {
        iri.strip_prefix(ENTITY_IRI_PREFIX)
    } else {
        None
    }
}

static DIRECT_PROPERTY_IRI_PREFIX: &str = "http://www.wikidata.org/prop/direct/";

fn direct_property(predicate: &str) -> Option<&str> {
    predicate.strip_prefix(DIRECT_PROPERTY_IRI_PREFIX)
}

pub fn unescape(s: &str) -> String {
    let mut chars = s.chars().enumerate();
    let mut res = String::with_capacity(s.len());

    while let Some((idx, c)) = chars.next() {
        if c == '\\' {
            match chars.next() {
                None => {
                    panic!("invalid escape at {} in {}", idx, s);
                }
                Some((idx, c2)) => {
                    res.push(match c2 {
                        't' => '\t',
                        'b' => '\u{08}',
                        'n' => '\n',
                        'r' => '\r',
                        'f' => '\u{0C}',
                        '\\' => '\\',

                        'u' => match parse_unicode(&mut chars, 4) {
                            Ok(c3) => c3,
                            Err(err) => {
                                panic!("invalid escape {}{} at {} in {}: {}", c, c2, idx, s, err);
                            }
                        },
                        'U' => match parse_unicode(&mut chars, 8) {
                            Ok(c3) => c3,
                            Err(err) => {
                                panic!("invalid escape {}{} at {} in {}: {}", c, c2, idx, s, err);
                            }
                        },
                        _ => {
                            panic!("invalid escape {}{} at {} in {}", c, c2, idx, s);
                        }
                    });
                    continue;
                }
            };
        }

        res.push(c);
    }

    res
}

fn parse_unicode<I>(chars: &mut I, count: usize) -> Result<char, String>
where
    I: Iterator<Item = (usize, char)>,
{
    let unicode_seq: String = chars.take(count).map(|(_, c)| c).collect();

    u32::from_str_radix(&unicode_seq, 16)
        .map_err(|e| format!("could not parse {} as u32 hex: {}", unicode_seq, e))
        .and_then(|u| {
            std::char::from_u32(u).ok_or_else(|| format!("could not parse {} as a unicode char", u))
        })
}

fn main() {
    let opts: Opts = Opts::parse();
    let labels = opts.labels;
    let statement_counts = opts.statement_counts;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        if r.load(Ordering::SeqCst) {
            exit(1);
        }
        r.store(false, Ordering::SeqCst);
    })
    .expect("failed to set Ctrl-C handler");

    let start = Instant::now();

    let (work_sender, work_receiver) = bounded::<Work>(0);
    let (result_sender, result_receiver) = unbounded();

    let mut threads = Vec::new();
    let thread_count = opts.threads.unwrap_or_else(|| num_cpus::get() * 2);
    for id in 1..=thread_count {
        let work_receiver = work_receiver.clone();
        let result_sender = result_sender.clone();
        threads.push(thread::spawn(move || {
            consume(
                id.to_string(),
                work_receiver,
                result_sender,
                labels,
                statement_counts,
            )
        }));
    }

    let mut exit_code = 0;

    for path in opts.paths {
        let file = File::open(&path).expect("can't open file");

        let decoder = BzDecoder::new(BufReader::new(file));
        eprintln!("# processing {}", path);

        let (finished, count) = produce(running.clone(), opts.skip, decoder, &work_sender);
        eprintln!("# processed {}: {}", path, count);

        if !finished {
            exit_code = 1;
            break;
        }
    }

    for _ in &threads {
        work_sender.send(Work::DONE).unwrap();
    }

    let mut statement_counter = HashMap::new();

    let mut result_count = 0;
    for result in result_receiver.iter() {
        if let Some(statement_counts) = result.statement_counts {
            for (id, count) in statement_counts.iter() {
                *statement_counter.entry(id.to_string()).or_insert(0) += count;
            }
        }

        result_count += 1;
        if result_count == thread_count {
            break;
        }
    }

    if statement_counts {
        eprintln!("# entities: {}", statement_counter.len());
        let path = "statement_counts.bz2";
        let file = File::create(path).unwrap_or_else(|_| panic!("unable to create file: {}", path));
        let mut encoder = BzEncoder::new(BufWriter::new(file), Compression::best());
        for (id, count) in statement_counter.iter() {
            encoder
                .write_fmt(format_args!("{} {}\n", id, count))
                .unwrap();
        }
        encoder.try_finish().unwrap();
    }

    let duration = start.elapsed();
    eprintln!("# took {:?}", duration);

    exit(exit_code);
}

#[cfg(test)]
mod tests {

    use super::*;
    use pretty_assertions::assert_eq;
    use std::fs::read_to_string;
    use std::io::{self, Lines};
    use std::path::{Path, PathBuf};

    #[test]
    fn test_literal_with_type() {
        let line = r#"<http://www.wikidata.org/entity/Q1644> <http://www.wikidata.org/prop/direct/P2043> "+1094.26"^^<http://www.w3.org/2001/XMLSchema#decimal> ."#;
        assert_eq!(
            parse(1, line, &RE),
            Statement {
                subject: Subject::IRI("http://www.wikidata.org/entity/Q1644"),
                predicate: "http://www.wikidata.org/prop/direct/P2043",
                object: Object::Literal(
                    "+1094.26",
                    Extra::Type("http://www.w3.org/2001/XMLSchema#decimal")
                )
            }
        );
    }

    #[test]
    fn test_literal_with_lang() {
        let line = r#"<http://www.wikidata.org/entity/Q177> <http://schema.org/name> "pizza"@en ."#;
        assert_eq!(
            parse(1, line, &RE),
            Statement {
                subject: Subject::IRI("http://www.wikidata.org/entity/Q177"),
                predicate: "http://schema.org/name",
                object: Object::Literal("pizza", Extra::Lang("en"))
            }
        );
    }

    #[test]
    fn test_literal() {
        let line = r#"<http://www.wikidata.org/entity/Q177> <http://www.wikidata.org/prop/direct/P373> "Pizzas" ."#;
        assert_eq!(
            parse(1, line, &RE),
            Statement {
                subject: Subject::IRI("http://www.wikidata.org/entity/Q177"),
                predicate: "http://www.wikidata.org/prop/direct/P373",
                object: Object::Literal("Pizzas", Extra::None)
            }
        );
    }

    #[test]
    fn test_blank_subject() {
        let line = r#"_:foo <bar> <baz>"#;
        assert_eq!(
            parse(1, line, &RE),
            Statement {
                subject: Subject::Blank("foo"),
                predicate: "bar",
                object: Object::IRI("baz")
            }
        );
    }

    #[test]
    fn test_blank_object() {
        let line = r#"<foo> <bar> _:baz"#;
        assert_eq!(
            parse(1, line, &RE),
            Statement {
                subject: Subject::IRI("foo"),
                predicate: "bar",
                object: Object::Blank("baz")
            }
        );
    }

    #[test]
    fn test_statement_count() {
        let a = format!("{}a", ENTITY_IRI_PREFIX);
        let b = format!("{}b", ENTITY_IRI_PREFIX);

        let first_predicate = format!("{}first", DIRECT_PROPERTY_IRI_PREFIX);
        let second_predicate = "second";
        let third_predicate = format!("{}third", DIRECT_PROPERTY_IRI_PREFIX);

        let first = Statement {
            subject: Subject::IRI(a.as_str()),
            predicate: first_predicate.as_str(),
            object: Object::IRI(""),
        };
        let second = Statement {
            subject: Subject::IRI(b.as_str()),
            predicate: second_predicate,
            object: Object::IRI(""),
        };
        let third = Statement {
            subject: Subject::IRI(a.as_str()),
            predicate: third_predicate.as_str(),
            object: Object::IRI(""),
        };
        let mut counter = HashMap::new();
        maybe_count_statement(Some(&mut counter), "a", first);
        maybe_count_statement(Some(&mut counter), "b", second);
        maybe_count_statement(Some(&mut counter), "a", third);
        assert_eq!(counter.len(), 1);
        assert_eq!(counter.get("a"), Some(&2));
        assert_eq!(counter.get("b"), None);
    }

    #[test]
    fn test_geo_literals() {
        assert!(is_acceptable(parse(
            1,
            r#"<foo> <bar> "Point(4.6681 50.6411)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> ."#,
            &RE,
        )));
        assert!(!is_acceptable(parse(
            1,
            r#"<foo> <bar> "<http://www.wikidata.org/entity/Q405> Point(-141.6 42.6)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> ."#,
            &RE,
        )));
    }

    fn read_lines<P>(filename: P) -> io::Result<Lines<BufReader<File>>>
    where
        P: AsRef<Path>,
    {
        let file = File::open(filename)?;
        Ok(BufReader::new(file).lines())
    }

    #[test]
    fn test_full() -> Result<(), ()> {
        let dir = env!("CARGO_MANIFEST_DIR");

        let mut in_path = PathBuf::from(dir);
        in_path.push("test.in.rdf");
        let in_path = in_path.as_os_str().to_str().unwrap();

        let mut out_path = PathBuf::from(dir);
        out_path.push("test.out.rdf");
        let out_path = out_path.as_os_str().to_str().unwrap();

        let mut lines_writer = Vec::new();
        let mut labels_writer = Vec::new();

        for (line, number) in read_lines(in_path).unwrap().zip(1u64..) {
            let mut line = line.unwrap();
            line.push('\n');
            handle(
                &mut lines_writer,
                Some(&mut labels_writer),
                None,
                number,
                line,
                &RE,
            );
        }

        let expected = read_to_string(out_path).unwrap();
        assert_eq!(String::from_utf8(lines_writer).unwrap(), expected);

        Ok(())
    }
}
