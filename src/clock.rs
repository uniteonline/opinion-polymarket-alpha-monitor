use crate::models::ExchangeTsSource;
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Debug)]
pub struct ClockOffset {
    offset_ms: AtomicI64,
    alpha: f64,
    max_step_ms: i64,
}

impl ClockOffset {
    pub fn new(alpha: f64, max_step_ms: i64) -> Self {
        ClockOffset {
            offset_ms: AtomicI64::new(0),
            alpha,
            max_step_ms,
        }
    }

    pub fn current(&self) -> i64 {
        self.offset_ms.load(Ordering::Relaxed)
    }

    pub fn apply_sample(&self, sample_offset: i64, source: ExchangeTsSource) -> (i64, i64) {
        assert!(matches!(
            source,
            ExchangeTsSource::VenueRest | ExchangeTsSource::VenueWs
        ));
        let current = self.current();
        let delta = sample_offset - current;
        let clamped = clamp(delta, -self.max_step_ms, self.max_step_ms);
        let updated = current + (self.alpha * clamped as f64).round() as i64;
        self.offset_ms.store(updated, Ordering::Relaxed);
        (updated, clamped)
    }
}

fn clamp(value: i64, min: i64, max: i64) -> i64 {
    if value < min {
        return min;
    }
    if value > max {
        return max;
    }
    value
}
