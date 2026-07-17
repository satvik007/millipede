use crate::{HeaderGenerator, HeaderProfile};

/// A v0.1 `post_page_create` consistency stub that produces a consistent user agent and
/// `Accept-*`/`Sec-Ch-Ua-*` header set only.
///
/// This generator does not patch navigator, canvas, or WebGL properties and makes no TLS
/// (JA3/JA4) fingerprinting claim.
#[derive(Debug, Clone, Default)]
pub struct BrowserFingerprintGenerator {
    headers: HeaderGenerator,
}

impl BrowserFingerprintGenerator {
    /// Creates a generator backed by the bundled browser header profiles.
    pub fn new() -> Self {
        Self::default()
    }

    /// Generates the header profile selected deterministically by `seed`.
    pub fn generate(&self, seed: &str) -> HeaderProfile {
        self.headers.generate(seed)
    }
}

#[cfg(test)]
mod tests {
    use crate::HeaderGenerator;

    use super::BrowserFingerprintGenerator;

    #[test]
    fn generate_delegates_deterministically() {
        let seed = "session-A";

        assert_eq!(
            BrowserFingerprintGenerator::new().generate(seed),
            HeaderGenerator::new().generate(seed)
        );
    }
}
