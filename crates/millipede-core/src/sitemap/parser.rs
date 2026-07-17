use std::{
    io::{self, BufReader, Read},
    sync::mpsc::{Receiver, SyncSender},
};

use bytes::Bytes;
use flate2::read::MultiGzDecoder;
use quick_xml::{Reader, events::Event};
use tokio::sync::mpsc;

use super::SitemapEntry;

const EVENT_CHANNEL_CAPACITY: usize = 8;
const CHUNK_CHANNEL_CAPACITY: usize = 1;

pub(crate) type EventResult = Result<SitemapEvent, SitemapParseError>;
pub(crate) type EventSender = mpsc::Sender<EventResult>;
pub(crate) type EventReceiver = mpsc::Receiver<EventResult>;

#[derive(Debug)]
pub(crate) enum SitemapEvent {
    Entry(SitemapEntry),
    Nested(String),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SitemapParseError {
    #[error("sitemap XML parse failed: {0}")]
    Xml(String),
    #[error("sitemap response body failed: {0}")]
    Body(String),
}

impl SitemapParseError {
    pub(crate) fn body(message: impl Into<String>) -> Self {
        Self::Body(message.into())
    }

    pub(crate) fn is_body(&self) -> bool {
        matches!(self, Self::Body(_))
    }
}

pub(crate) struct XmlPump;

impl XmlPump {
    pub(crate) fn spawn(gzip: bool) -> (SyncSender<Bytes>, EventSender, EventReceiver) {
        let (chunk_tx, chunk_rx) = std::sync::mpsc::sync_channel(CHUNK_CHANNEL_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let pump_tx = event_tx.clone();
        tokio::task::spawn_blocking(move || {
            let reader = ChunkReader::new(chunk_rx);
            if gzip {
                parse(BufReader::new(MultiGzDecoder::new(reader)), pump_tx);
            } else {
                parse(BufReader::new(reader), pump_tx);
            }
        });
        (chunk_tx, event_tx, event_rx)
    }
}

struct ChunkReader {
    receiver: Receiver<Bytes>,
    current: Bytes,
}

impl ChunkReader {
    fn new(receiver: Receiver<Bytes>) -> Self {
        Self {
            receiver,
            current: Bytes::new(),
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.current.is_empty() {
            match self.receiver.recv() {
                Ok(chunk) => self.current = chunk,
                Err(_) => return Ok(0),
            }
        }
        let length = buffer.len().min(self.current.len());
        buffer[..length].copy_from_slice(&self.current[..length]);
        self.current = self.current.slice(length..);
        Ok(length)
    }
}

#[derive(Default)]
struct EntryBuilder {
    loc: Option<String>,
    lastmod: Option<String>,
    priority: Option<String>,
    changefreq: Option<String>,
    malformed: bool,
}

impl EntryBuilder {
    fn into_entry(self) -> Option<SitemapEntry> {
        if self.malformed {
            return None;
        }
        let loc = self.loc?;
        let priority = match self.priority {
            Some(value) => match value.parse() {
                Ok(value) => Some(value),
                Err(_) => return None,
            },
            None => None,
        };
        Some(SitemapEntry {
            loc,
            lastmod: self.lastmod,
            priority,
            changefreq: self.changefreq,
        })
    }
}

fn parse<R: io::BufRead>(reader: R, sender: mpsc::Sender<Result<SitemapEvent, SitemapParseError>>) {
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    // Broken end tags invalidate only their containing entry where possible.
    xml.config_mut().check_end_names = false;
    let mut buffer = Vec::new();
    let mut path = Vec::<Vec<u8>>::new();
    let mut entry_element: Option<(usize, Vec<u8>)> = None;
    let mut sitemap_element: Option<(usize, Vec<u8>)> = None;
    let mut entry: Option<EntryBuilder> = None;
    let mut sitemap_loc: Option<String> = None;
    let mut sitemap_malformed = false;

    loop {
        match xml.read_event_into(&mut buffer) {
            Ok(Event::Start(start)) => {
                let qualified_name = start.name().as_ref().to_vec();
                let name = local_name(&qualified_name);
                if name == b"url"
                    && path
                        .last()
                        .is_some_and(|item| local_name(item) == b"urlset")
                {
                    entry = Some(EntryBuilder::default());
                    entry_element = Some((path.len() + 1, qualified_name.clone()));
                } else if name == b"sitemap"
                    && path
                        .last()
                        .is_some_and(|item| local_name(item) == b"sitemapindex")
                {
                    sitemap_loc = None;
                    sitemap_malformed = false;
                    sitemap_element = Some((path.len() + 1, qualified_name.clone()));
                }
                path.push(qualified_name);
            }
            Ok(Event::Empty(empty)) => {
                let qualified_name = empty.name();
                let name = local_name(qualified_name.as_ref());
                if name == b"url"
                    && path
                        .last()
                        .is_some_and(|item| local_name(item) == b"urlset")
                {
                    // An empty URL has no location and is deliberately skipped.
                }
            }
            Ok(Event::Text(text)) => {
                let value = match text.unescape() {
                    Ok(value) => value.trim().to_owned(),
                    Err(_) => {
                        mark_malformed(&mut entry);
                        mark_sitemap_malformed(&path, &mut sitemap_malformed);
                        buffer.clear();
                        continue;
                    }
                };
                assign_text(&path, value, &mut entry, &mut sitemap_loc);
            }
            Ok(Event::CData(text)) => {
                let value = String::from_utf8_lossy(text.as_ref()).trim().to_owned();
                assign_text(&path, value, &mut entry, &mut sitemap_loc);
            }
            Ok(Event::End(end)) => {
                let qualified_name = end.name();
                let qualified_name = qualified_name.as_ref();
                if path
                    .last()
                    .is_some_and(|open| open.as_slice() != qualified_name)
                {
                    mark_malformed(&mut entry);
                    mark_sitemap_malformed(&path, &mut sitemap_malformed);
                }
                let closes_entry = entry_element.as_ref().is_some_and(|(depth, open)| {
                    *depth == path.len() && open.as_slice() == qualified_name
                });
                let closes_sitemap = sitemap_element.as_ref().is_some_and(|(depth, open)| {
                    *depth == path.len() && open.as_slice() == qualified_name
                });
                if closes_entry {
                    entry_element = None;
                    if let Some(candidate) = entry.take() {
                        if let Some(entry) = candidate.into_entry() {
                            if sender
                                .blocking_send(Ok(SitemapEvent::Entry(entry)))
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                } else if closes_sitemap {
                    sitemap_element = None;
                    if !sitemap_malformed {
                        if let Some(loc) = sitemap_loc.take() {
                            if sender.blocking_send(Ok(SitemapEvent::Nested(loc))).is_err() {
                                return;
                            }
                        }
                    }
                    sitemap_loc = None;
                    sitemap_malformed = false;
                }
                if let Some(position) = path
                    .iter()
                    .rposition(|item| item.as_slice() == qualified_name)
                {
                    path.truncate(position);
                }
            }
            Ok(Event::Eof) => return,
            Ok(_) => {}
            Err(error) => {
                let _ = sender.blocking_send(Err(SitemapParseError::Xml(error.to_string())));
                return;
            }
        }
        buffer.clear();
    }
}

fn assign_text(
    path: &[Vec<u8>],
    value: String,
    entry: &mut Option<EntryBuilder>,
    sitemap_loc: &mut Option<String>,
) {
    let Some(name) = path.last().map(|item| local_name(item)) else {
        return;
    };
    let parent = path
        .len()
        .checked_sub(2)
        .and_then(|index| path.get(index))
        .map(|item| local_name(item));
    if parent == Some(b"url") {
        if let Some(candidate) = entry.as_mut() {
            match name {
                b"loc" => append_text(&mut candidate.loc, value),
                b"lastmod" => append_text(&mut candidate.lastmod, value),
                b"changefreq" => append_text(&mut candidate.changefreq, value),
                b"priority" => append_text(&mut candidate.priority, value),
                _ => {}
            }
        }
    } else if name == b"loc" && parent == Some(b"sitemap") {
        append_text(sitemap_loc, value);
    }
}

fn append_text(target: &mut Option<String>, value: String) {
    if let Some(current) = target {
        current.push_str(&value);
    } else {
        *target = Some(value);
    }
}

fn mark_malformed(entry: &mut Option<EntryBuilder>) {
    if let Some(candidate) = entry.as_mut() {
        candidate.malformed = true;
    }
}

fn mark_sitemap_malformed(path: &[Vec<u8>], malformed: &mut bool) {
    if path.iter().any(|name| local_name(name) == b"sitemap") {
        *malformed = true;
    }
}

fn local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}
