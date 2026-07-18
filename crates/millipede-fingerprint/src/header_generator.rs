use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::OnceLock,
};

const HEADER_PROFILES_JSON: &str = include_str!("../data/header_profiles.json");

/// A browser user agent and its ordered companion HTTP headers.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeaderProfile {
    /// Browser user-agent value.
    pub user_agent: String,
    /// Ordered header name and value pairs associated with the user agent.
    pub headers: Vec<(String, String)>,
}

fn bundled_profiles() -> &'static [HeaderProfile] {
    static PROFILES: OnceLock<Vec<HeaderProfile>> = OnceLock::new();
    PROFILES
        .get_or_init(|| {
            serde_json::from_str(HEADER_PROFILES_JSON)
                .expect("bundled header_profiles.json must parse")
        })
        .as_slice()
}

/// Deterministically selects browser-like header profiles from a curated dataset.
#[derive(Debug, Clone)]
pub struct HeaderGenerator {
    profiles: Vec<HeaderProfile>,
}

impl HeaderGenerator {
    /// Creates a generator backed by the bundled profile dataset.
    pub fn new() -> Self {
        Self {
            profiles: bundled_profiles().to_vec(),
        }
    }

    /// Creates a generator backed by caller-supplied profiles.
    ///
    /// # Panics
    ///
    /// Panics if `profiles` is empty.
    pub fn from_profiles(profiles: Vec<HeaderProfile>) -> Self {
        assert!(!profiles.is_empty(), "header profiles must not be empty");
        Self { profiles }
    }

    /// Returns the profile selected deterministically by `seed`.
    pub fn generate(&self, seed: &str) -> HeaderProfile {
        let mut hasher = DefaultHasher::new();
        seed.as_bytes().hash(&mut hasher);
        let index = hasher.finish() as usize % self.profiles.len();
        self.profiles[index].clone()
    }
}

impl Default for HeaderGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::HeaderGenerator;

    #[test]
    fn same_seed_is_deterministic_across_fresh_instances() {
        assert_eq!(
            HeaderGenerator::new().generate("session-A"),
            HeaderGenerator::new().generate("session-A")
        );
    }

    #[test]
    fn same_seed_is_byte_identical_when_serialized() {
        let first = serde_json::to_vec(&HeaderGenerator::new().generate("session-A")).unwrap();
        let second = serde_json::to_vec(&HeaderGenerator::new().generate("session-A")).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn different_seeds_can_select_different_profiles() {
        let generator = HeaderGenerator::new();
        let profiles = (0..20)
            .map(|index| generator.generate(&format!("session-{index}")))
            .collect::<Vec<_>>();

        assert!(profiles.iter().any(|profile| profile != &profiles[0]));
    }

    #[test]
    fn bundled_dataset_has_multiple_distinct_profiles() {
        let generator = HeaderGenerator::new();
        let user_agents = (0..200)
            .map(|index| generator.generate(&format!("session-{index}")).user_agent)
            .collect::<HashSet<_>>();

        assert!(user_agents.len() >= 6);
    }

    #[test]
    #[should_panic(expected = "header profiles must not be empty")]
    fn from_profiles_rejects_empty() {
        HeaderGenerator::from_profiles(vec![]);
    }

    #[test]
    fn generated_profile_is_well_formed() {
        let profile = HeaderGenerator::new().generate("well-formed");

        assert!(!profile.user_agent.is_empty());
        assert!(!profile.headers.is_empty());
        assert!(
            profile
                .headers
                .iter()
                .all(|(name, value)| !name.is_empty() && !value.is_empty())
        );
    }
}
