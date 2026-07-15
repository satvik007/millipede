#![allow(dead_code)]

use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::time::Instant;

struct TaskRateState {
    tokens: f64,
    last_refill: Instant,
}

pub(crate) struct TaskRateLimiter {
    capacity: f64,
    refill_per_sec: f64,
    state: Mutex<TaskRateState>,
}

impl TaskRateLimiter {
    pub(crate) fn new(max_tasks_per_minute: u32) -> Self {
        let capacity = max_tasks_per_minute.max(1) as f64;
        Self {
            capacity,
            refill_per_sec: capacity / 60.0,
            state: Mutex::new(TaskRateState {
                tokens: 1.0,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Consumes a token or returns the duration until the next token is available.
    pub(crate) fn try_acquire(&self, now: Instant) -> Result<(), Duration> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let elapsed = now.saturating_duration_since(state.last_refill);
        state.tokens =
            (state.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
        state.last_refill = now;

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            Ok(())
        } else {
            let wait = Duration::from_secs_f64((1.0 - state.tokens) / self.refill_per_sec);
            Err(wait)
        }
    }
}

#[derive(Default)]
struct DomainState {
    next_allowed: Option<Instant>,
    last_reserved: Option<Instant>,
    consecutive_429s: u32,
    penalty: Duration,
    floor: Duration,
}

pub(crate) struct DomainLimiter {
    same_domain_delay: Duration,
    state: Mutex<HashMap<String, DomainState>>,
}

impl DomainLimiter {
    pub(crate) fn new(same_domain_delay: Duration) -> Self {
        Self {
            same_domain_delay,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Reserves the next host slot and returns how long the caller should wait.
    pub(crate) fn reserve_slot(&self, host: &str, now: Instant) -> Duration {
        {
            let mut states = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let state = states.entry(host.to_owned()).or_default();
            let delay = self
                .same_domain_delay
                .max(state.floor)
                .saturating_add(state.penalty);
            let slot = state.next_allowed.unwrap_or(now).max(now);
            let wait = slot.saturating_duration_since(now);
            state.next_allowed = Some(slot + delay);
            state.last_reserved = Some(slot);
            wait
        }
    }

    /// Updates a host's backoff state from an HTTP response and optional Retry-After value.
    pub(crate) fn note_response(
        &self,
        host: &str,
        status: Option<http::StatusCode>,
        retry_after: Option<Duration>,
        now: Instant,
    ) {
        let mut states = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let state = states.entry(host.to_owned()).or_default();

        if status == Some(http::StatusCode::TOO_MANY_REQUESTS) {
            state.consecutive_429s = state.consecutive_429s.saturating_add(1);
            let exponent = state.consecutive_429s.saturating_sub(1).min(31);
            let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
            let base = self.same_domain_delay.max(Duration::from_secs(1));
            let penalty = base
                .checked_mul(multiplier)
                .unwrap_or(Duration::MAX)
                .min(Duration::from_secs(5 * 60));
            state.penalty = penalty;
            let baseline = state.next_allowed.unwrap_or(now).max(now);
            state.next_allowed = Some(baseline + penalty);
        } else if status.is_some() {
            state.consecutive_429s = 0;
            state.penalty = Duration::ZERO;
            state.next_allowed = state
                .last_reserved
                .map(|last_reserved| last_reserved + self.same_domain_delay.max(state.floor));
        }

        if let Some(retry_after) = retry_after {
            raise_next_allowed(state, now + retry_after);
        }
    }

    /// Sets a persistent minimum inter-request delay for one host.
    pub(crate) fn set_delay_floor(&self, host: &str, floor: Duration) {
        let mut states = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let state = states.entry(host.to_owned()).or_default();
        state.floor = floor;
        if let Some(last_reserved) = state.last_reserved {
            let delay = self
                .same_domain_delay
                .max(state.floor)
                .saturating_add(state.penalty);
            raise_next_allowed(state, last_reserved + delay);
        }
    }
}

fn raise_next_allowed(state: &mut DomainState, candidate: Instant) {
    state.next_allowed = Some(match state.next_allowed {
        Some(current) => current.max(candidate),
        None => candidate,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn task_rate_limiter_throttles_and_recovers() {
        let limiter = TaskRateLimiter::new(60);
        let now = Instant::now();
        assert_eq!(limiter.try_acquire(now), Ok(()));
        let wait = limiter.try_acquire(now).unwrap_err();
        assert_eq!(wait, Duration::from_secs(1));

        tokio::time::advance(wait).await;
        assert_eq!(limiter.try_acquire(Instant::now()), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn domain_limiter_enforces_same_domain_delay() {
        let limiter = DomainLimiter::new(Duration::from_millis(200));
        let now = Instant::now();
        assert_eq!(limiter.reserve_slot("a.example", now), Duration::ZERO);
        assert_eq!(
            limiter.reserve_slot("a.example", now),
            Duration::from_millis(200)
        );
        assert_eq!(limiter.reserve_slot("b.example", now), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn domain_limiter_extends_on_retry_after_and_429() {
        let base = Duration::from_millis(200);
        let limiter = DomainLimiter::new(base);
        let now = Instant::now();
        limiter.note_response(
            "example.com",
            Some(http::StatusCode::TOO_MANY_REQUESTS),
            Some(Duration::from_secs(2)),
            now,
        );
        let wait = limiter.reserve_slot("example.com", now);
        assert!(wait >= Duration::from_secs(2));

        tokio::time::advance(wait).await;
        let now = Instant::now();
        limiter.note_response("example.com", Some(http::StatusCode::OK), None, now);
        assert_eq!(limiter.reserve_slot("example.com", now), base);
        let states = limiter.state.lock().unwrap();
        assert_eq!(states["example.com"].consecutive_429s, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn domain_limiter_adds_429_penalty_after_reserved_slots() {
        let delay = Duration::from_secs(1);
        let limiter = DomainLimiter::new(delay);
        let now = Instant::now();
        assert_eq!(limiter.reserve_slot("example.com", now), Duration::ZERO);
        assert_eq!(limiter.reserve_slot("example.com", now), delay);

        limiter.note_response(
            "example.com",
            Some(http::StatusCode::TOO_MANY_REQUESTS),
            None,
            now,
        );

        let states = limiter.state.lock().unwrap();
        assert_eq!(states["example.com"].next_allowed.unwrap() - now, delay * 3);
    }

    #[tokio::test(start_paused = true)]
    async fn domain_limiter_staggers_back_to_back_reservations_during_429_penalty() {
        let base = Duration::from_millis(200);
        let penalty = Duration::from_secs(1);
        let limiter = DomainLimiter::new(base);
        let now = Instant::now();
        limiter.note_response(
            "example.com",
            Some(http::StatusCode::TOO_MANY_REQUESTS),
            None,
            now,
        );

        let first_wait = limiter.reserve_slot("example.com", now);
        let second_wait = limiter.reserve_slot("example.com", now);

        assert_eq!(first_wait, penalty);
        assert_eq!(second_wait - first_wait, base + penalty);
    }

    #[tokio::test(start_paused = true)]
    async fn domain_limiter_set_delay_floor_applies_to_the_next_reservation() {
        let limiter = DomainLimiter::new(Duration::ZERO);
        let now = Instant::now();
        assert_eq!(limiter.reserve_slot("example.com", now), Duration::ZERO);
        limiter.set_delay_floor("example.com", Duration::from_millis(500));
        assert_eq!(
            limiter.reserve_slot("example.com", now),
            Duration::from_millis(500)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_429s_back_off_exponentially_and_cap() {
        let limiter = DomainLimiter::new(Duration::ZERO);
        let now = Instant::now();
        let mut penalties = Vec::new();
        let mut previous = now;
        for _ in 0..3 {
            limiter.note_response(
                "example.com",
                Some(http::StatusCode::TOO_MANY_REQUESTS),
                None,
                now,
            );
            let states = limiter.state.lock().unwrap();
            let next_allowed = states["example.com"].next_allowed.unwrap();
            penalties.push(next_allowed - previous);
            previous = next_allowed;
        }
        assert_eq!(
            penalties,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4)
            ]
        );

        for _ in 0..20 {
            limiter.note_response(
                "example.com",
                Some(http::StatusCode::TOO_MANY_REQUESTS),
                None,
                now,
            );
        }
        let states = limiter.state.lock().unwrap();
        let next_allowed = states["example.com"].next_allowed.unwrap();
        drop(states);
        limiter.note_response(
            "example.com",
            Some(http::StatusCode::TOO_MANY_REQUESTS),
            None,
            now,
        );
        let states = limiter.state.lock().unwrap();
        assert_eq!(
            states["example.com"].next_allowed.unwrap() - next_allowed,
            Duration::from_secs(5 * 60)
        );
    }
}
