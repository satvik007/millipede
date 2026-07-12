use millipede_core::request::Request;
use std::collections::{HashMap, VecDeque};

/// Ordering policy used by an in-memory request queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MemoryQueuePolicy {
    /// Process requests in first-in, first-out order.
    #[default]
    Fifo,
    /// Alternate between URL hosts while preserving per-host order.
    DomainRoundRobin,
}

#[derive(Debug)]
pub(crate) enum Frontier {
    Fifo(VecDeque<Request>),
    DomainRoundRobin(DomainRoundRobin),
}

impl Frontier {
    pub(crate) fn new(policy: MemoryQueuePolicy) -> Self {
        match policy {
            MemoryQueuePolicy::Fifo => Self::Fifo(VecDeque::new()),
            MemoryQueuePolicy::DomainRoundRobin => {
                Self::DomainRoundRobin(DomainRoundRobin::default())
            }
        }
    }

    pub(crate) fn push_back(&mut self, request: Request) {
        match self {
            Self::Fifo(requests) => requests.push_back(request),
            Self::DomainRoundRobin(requests) => requests.push_back(request),
        }
    }

    pub(crate) fn push_front(&mut self, request: Request) {
        match self {
            Self::Fifo(requests) => requests.push_front(request),
            Self::DomainRoundRobin(requests) => requests.push_front(request),
        }
    }

    pub(crate) fn pop_front(&mut self) -> Option<Request> {
        match self {
            Self::Fifo(requests) => requests.pop_front(),
            Self::DomainRoundRobin(requests) => requests.pop_front(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Fifo(requests) => requests.len(),
            Self::DomainRoundRobin(requests) => requests.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Fifo(requests) => requests.is_empty(),
            Self::DomainRoundRobin(requests) => requests.is_empty(),
        }
    }
}

/// A pure frontier that rotates fairly between URL hosts.
#[derive(Debug, Default)]
pub struct DomainRoundRobin {
    by_host: HashMap<String, VecDeque<Request>>,
    rotation: VecDeque<String>,
}

impl DomainRoundRobin {
    fn host_key(request: &Request) -> String {
        request.url.host_str().unwrap_or("").to_ascii_lowercase()
    }

    fn push_back(&mut self, request: Request) {
        let host = Self::host_key(&request);
        if let Some(requests) = self.by_host.get_mut(&host) {
            requests.push_back(request);
        } else {
            self.by_host.insert(host.clone(), VecDeque::from([request]));
            self.rotation.push_back(host);
        }
    }

    fn push_front(&mut self, request: Request) {
        let host = Self::host_key(&request);
        if let Some(requests) = self.by_host.get_mut(&host) {
            requests.push_front(request);
        } else {
            self.by_host.insert(host.clone(), VecDeque::from([request]));
            self.rotation.push_front(host);
        }
    }

    fn pop_front(&mut self) -> Option<Request> {
        let host = self.rotation.pop_front()?;
        let requests = self
            .by_host
            .get_mut(&host)
            .expect("rotation only contains active hosts");
        let request = requests.pop_front();
        if requests.is_empty() {
            self.by_host.remove(&host);
        } else {
            self.rotation.push_back(host);
        }
        request
    }

    fn len(&self) -> usize {
        self.by_host.values().map(VecDeque::len).sum()
    }

    fn is_empty(&self) -> bool {
        self.by_host.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::DomainRoundRobin;
    use millipede_core::request::Request;

    fn request(url: &str) -> Request {
        Request::get(url).build().unwrap()
    }

    fn host(frontier: &mut DomainRoundRobin) -> String {
        frontier
            .pop_front()
            .unwrap()
            .url
            .host_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn strictly_rotates_between_hosts() {
        let mut frontier = DomainRoundRobin::default();
        for url in [
            "https://a.com/1",
            "https://a.com/2",
            "https://a.com/3",
            "https://b.com/1",
            "https://b.com/2",
            "https://c.com/1",
        ] {
            frontier.push_back(request(url));
        }

        let hosts = (0..6).map(|_| host(&mut frontier)).collect::<Vec<_>>();
        assert_eq!(
            hosts,
            ["a.com", "b.com", "c.com", "a.com", "b.com", "a.com"]
        );
    }

    #[test]
    fn push_front_heads_new_host_queue_and_rotation() {
        let mut frontier = DomainRoundRobin::default();
        frontier.push_back(request("https://a.com/old"));
        frontier.push_front(request("https://b.com/front"));

        assert_eq!(frontier.pop_front().unwrap().url.path(), "/front");
        assert_eq!(frontier.pop_front().unwrap().url.path(), "/old");
    }

    #[test]
    fn push_front_precedes_items_for_an_existing_host() {
        let mut frontier = DomainRoundRobin::default();
        frontier.push_back(request("https://a.com/old-1"));
        frontier.push_back(request("https://a.com/old-2"));
        frontier.push_front(request("https://a.com/front"));

        assert_eq!(frontier.pop_front().unwrap().url.path(), "/front");
        assert_eq!(frontier.pop_front().unwrap().url.path(), "/old-1");
        assert_eq!(frontier.pop_front().unwrap().url.path(), "/old-2");
    }

    #[test]
    fn exhausted_host_leaves_rotation() {
        let mut frontier = DomainRoundRobin::default();
        for url in [
            "https://a.com/1",
            "https://b.com/1",
            "https://c.com/1",
            "https://a.com/2",
            "https://b.com/2",
            "https://a.com/3",
            "https://b.com/3",
        ] {
            frontier.push_back(request(url));
        }

        assert_eq!(host(&mut frontier), "a.com");
        assert_eq!(host(&mut frontier), "b.com");
        assert_eq!(host(&mut frontier), "c.com");
        let remaining = (0..4).map(|_| host(&mut frontier)).collect::<Vec<_>>();
        assert_eq!(remaining, ["a.com", "b.com", "a.com", "b.com"]);
    }

    #[test]
    fn tracks_length_and_emptiness() {
        let mut frontier = DomainRoundRobin::default();
        assert!(frontier.is_empty());
        assert_eq!(frontier.len(), 0);
        frontier.push_back(request("https://a.com/1"));
        frontier.push_back(request("https://b.com/1"));
        assert!(!frontier.is_empty());
        assert_eq!(frontier.len(), 2);
        frontier.pop_front();
        assert_eq!(frontier.len(), 1);
        frontier.pop_front();
        assert!(frontier.is_empty());
    }
}
