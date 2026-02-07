use crate::models::ExchangeTsSource;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ts_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub struct TimestampCheck {
    pub exchange_ts_ms: i64,
    pub source: ExchangeTsSource,
    pub ts_missing: bool,
    pub ts_anomaly: bool,
    pub out_of_order: bool,
}

fn normalize_ts_ms(ts_raw: i64) -> i64 {
    if ts_raw < 0 {
        return ts_raw;
    }
    if ts_raw < 100_000_000_000 {
        // seconds -> ms
        return ts_raw * 1000;
    }
    if ts_raw < 100_000_000_000_000 {
        // milliseconds
        return ts_raw;
    }
    if ts_raw < 100_000_000_000_000_000 {
        // microseconds -> ms
        return ts_raw / 1000;
    }
    // nanoseconds -> ms
    ts_raw / 1_000_000
}

pub fn validate_exchange_ts(
    ts_raw: Option<i64>,
    local_ts_ms: i64,
    last_good_ts_ms: Option<i64>,
) -> TimestampCheck {
    if let Some(mut ts) = ts_raw {
        ts = normalize_ts_ms(ts);
        let out_of_order = last_good_ts_ms.map(|last| ts < last).unwrap_or(false);
        if ts < 0 {
            return TimestampCheck {
                exchange_ts_ms: local_ts_ms,
                source: ExchangeTsSource::LocalFallback,
                ts_missing: false,
                ts_anomaly: true,
                out_of_order,
            };
        }
        if ts > local_ts_ms + 300_000 {
            return TimestampCheck {
                exchange_ts_ms: local_ts_ms,
                source: ExchangeTsSource::LocalFallback,
                ts_missing: false,
                ts_anomaly: true,
                out_of_order,
            };
        }
        if let Some(last_good) = last_good_ts_ms {
            if ts < last_good - 2000 {
                return TimestampCheck {
                    exchange_ts_ms: local_ts_ms,
                    source: ExchangeTsSource::LocalFallback,
                    ts_missing: false,
                    ts_anomaly: true,
                    out_of_order: true,
                };
            }
        }
        return TimestampCheck {
            exchange_ts_ms: ts,
            source: ExchangeTsSource::VenueWs,
            ts_missing: false,
            ts_anomaly: false,
            out_of_order,
        };
    }
    TimestampCheck {
        exchange_ts_ms: local_ts_ms,
        source: ExchangeTsSource::LocalFallback,
        ts_missing: true,
        ts_anomaly: false,
        out_of_order: false,
    }
}
