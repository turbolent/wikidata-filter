use bzip2::bufread::BzDecoder;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use clap::Clap;
use crossbeam_channel::{bounded, Receiver, Sender};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::{BufRead, BufWriter, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const BATCH_SIZE: u64 = 100;

#[macro_use]
extern crate lazy_static_include;

#[derive(Clap)]
#[clap()]
struct Opts {
    #[clap(short, long)]
    labels: bool,
    #[clap(short, long)]
    skip: u64,
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

pub fn parse(line: u64, input: &str) -> Statement {
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
    let captures = RE
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

fn produce<T: Read>(running: Arc<AtomicBool>, skip: u64, reader: T, s: &Sender<Work>) -> u64 {
    let mut total = 0;
    let mut buf_reader = BufReader::new(reader);

    let mut lines = Vec::new();

    if skip > 0 {
        eprintln!("# skipping {}", skip)
    }

    loop {
        if !running.load(Ordering::SeqCst) {
            eprintln!("# interrupted after {}", total);
            break;
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

        if total % 100000 == 0 {
            let status = if skipped { "skipped" } else { "" };
            eprintln!("# {} {}", status, total);
        }
    }

    if !lines.is_empty() {
        s.send(Work::LINES(total, lines)).unwrap();
    }

    total
}

fn consume(name: String, r: Receiver<Work>, labels: bool) {
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

    loop {
        match r.recv().unwrap() {
            Work::LINES(number, lines) => {
                for line in lines {
                    handle(
                        &mut lines_encoder,
                        &mut labels_encoder.as_mut(),
                        number,
                        line,
                    )
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
                return;
            }
        }
    }
}

fn handle<T: Write, U: Write>(
    lines_writer: &mut T,
    labels_writer: &mut Option<&mut U>,
    number: u64,
    line: String,
) {
    let statement = parse(number, &line);

    if is_acceptable(statement) {
        lines_writer.write_all(line.as_bytes()).unwrap();
    }

    if let Some(labels_writer) = labels_writer.as_mut() {
        if let Some((iri, label)) = label(statement) {
            labels_writer
                .write_fmt(format_args!("{} {}\n", iri, label))
                .unwrap()
        }
    }
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

fn label(statement: Statement) -> Option<(&str, &str)> {
    if !LABELS.contains(statement.predicate) {
        return None;
    }

    if let Object::Literal(label, Extra::Lang(lang)) = statement.object {
        if LANGUAGES.contains(lang) {
            if let Subject::IRI(iri) = statement.subject {
                return Some((iri, label));
            }
        }
    }

    None
}

fn main() {
    let opts: Opts = Opts::parse();
    let labels = opts.labels;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("failed to set Ctrl-C handler");

    let start = Instant::now();

    let (line_sender, line_receiver) = bounded::<Work>(0);

    let mut threads = Vec::new();
    let thread_count = num_cpus::get();
    for id in 1..=thread_count {
        let line_receiver = line_receiver.clone();
        threads.push(thread::spawn(move || {
            consume(id.to_string(), line_receiver, labels)
        }));
    }

    for path in opts.paths {
        let file = File::open(&path).expect("can't open file");

        let decoder = BzDecoder::new(BufReader::new(file));
        eprintln!("# processing {}", path);

        let count = produce(running.clone(), opts.skip, decoder, &line_sender);
        eprintln!("# processed {}: {}", path, count);
    }

    for _ in &threads {
        line_sender.send(Work::DONE).unwrap();
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let duration = start.elapsed();
    eprintln!("# took {:?}", duration);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_with_type() {
        let line = r#"<http://www.wikidata.org/entity/Q1644> <http://www.wikidata.org/prop/direct/P2043> "+1094.26"^^<http://www.w3.org/2001/XMLSchema#decimal> ."#;
        assert_eq!(
            parse(1, line),
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
            parse(1, line),
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
            parse(1, line),
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
            parse(1, line),
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
            parse(1, line),
            Statement {
                subject: Subject::IRI("foo"),
                predicate: "bar",
                object: Object::Blank("baz")
            }
        );
    }

    #[test]
    fn test_geo_literals() {
        assert!(is_acceptable(parse(
            1,
            r#"<foo> <bar> "Point(4.6681 50.6411)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> ."#
        )));
        assert!(!is_acceptable(parse(
            1,
            r#"<foo> <bar> "<http://www.wikidata.org/entity/Q405> Point(-141.6 42.6)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> ."#
        )));
    }
}
