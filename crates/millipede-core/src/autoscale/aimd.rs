use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct UpdateGuard<'a>(&'a AtomicBool);

impl Drop for UpdateGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// An atomic additive-increase, multiplicative-decrease concurrency controller.
pub struct AimdController {
    min: usize,
    max: usize,
    increase_after_successes: usize,
    decrease_factor: f32,
    desired: AtomicUsize,
    success_streak: AtomicUsize,
    updating: AtomicBool,
}

impl AimdController {
    /// Creates a controller with normalized bounds and tuning values.
    pub fn new(
        min_concurrency: usize,
        max_concurrency: usize,
        initial_concurrency: usize,
        increase_after_successes: usize,
        decrease_factor: f32,
    ) -> Self {
        let min = min_concurrency.max(1);
        let max = max_concurrency.max(min);
        let decrease_factor = if decrease_factor > 0.0 && decrease_factor <= 1.0 {
            decrease_factor
        } else {
            0.5
        };
        Self {
            min,
            max,
            increase_after_successes: increase_after_successes.max(1),
            decrease_factor,
            desired: AtomicUsize::new(initial_concurrency.clamp(min, max)),
            success_streak: AtomicUsize::new(0),
            updating: AtomicBool::new(false),
        }
    }

    /// Returns the currently desired concurrency.
    pub fn desired_concurrency(&self) -> usize {
        self.desired.load(Ordering::Acquire)
    }

    /// Records a successful attempt, additively increasing after a sustained streak.
    pub fn record_success(&self) {
        let _update = self.begin_update();
        let previous = self
            .success_streak
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |streak| {
                let next = streak.saturating_add(1);
                Some(if next >= self.increase_after_successes {
                    next - self.increase_after_successes
                } else {
                    next
                })
            })
            .expect("success streak update always produces a value");
        if previous >= self.increase_after_successes - 1 {
            let _ = self
                .desired
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |desired| {
                    Some(desired.saturating_add(1).min(self.max))
                });
        }
    }

    /// Records a setback, clearing the streak and multiplicatively decreasing concurrency.
    pub fn record_setback(&self) {
        let _update = self.begin_update();
        self.success_streak.store(0, Ordering::Release);
        let _ = self
            .desired
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |desired| {
                Some(
                    ((desired as f32 * self.decrease_factor).round() as usize)
                        .clamp(self.min, self.max),
                )
            });
    }

    fn begin_update(&self) -> UpdateGuard<'_> {
        while self
            .updating
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        UpdateGuard(&self.updating)
    }
}
