use crate::models::{ConnectionHealthRow, Venue};
use crate::time_utils::now_ts_ms;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub enum WsState {
    Down = 0,
    Up = 1,
    Reconnecting = 2,
}

impl Default for WsState {
    fn default() -> Self {
        WsState::Down
    }
}

#[derive(Debug, Default, Clone)]
struct VenueHealth {
    ws_state: WsState,
    reconnect_count: i64,
    last_msg_local_ts: Option<i64>,
    heartbeat_sent: i64,
    heartbeat_fail: i64,
    dropped_events: i64,
}

#[derive(Debug, Default)]
pub struct HealthState {
    polymarket: VenueHealth,
    opinion: VenueHealth,
}

impl HealthState {
    pub fn new() -> Self {
        HealthState::default()
    }

    fn select_mut(&mut self, venue: Venue) -> &mut VenueHealth {
        match venue {
            Venue::Polymarket => &mut self.polymarket,
            Venue::Opinion => &mut self.opinion,
        }
    }

    fn select(&self, venue: Venue) -> &VenueHealth {
        match venue {
            Venue::Polymarket => &self.polymarket,
            Venue::Opinion => &self.opinion,
        }
    }

    pub fn set_state(&mut self, venue: Venue, state: WsState) {
        let entry = self.select_mut(venue);
        entry.ws_state = state;
    }

    pub fn record_reconnect(&mut self, venue: Venue) {
        let entry = self.select_mut(venue);
        entry.reconnect_count += 1;
        entry.ws_state = WsState::Reconnecting;
    }

    pub fn record_message(&mut self, venue: Venue) {
        let entry = self.select_mut(venue);
        entry.last_msg_local_ts = Some(now_ts_ms());
    }

    pub fn record_heartbeat_sent(&mut self, venue: Venue) {
        let entry = self.select_mut(venue);
        entry.heartbeat_sent += 1;
    }

    pub fn record_heartbeat_fail(&mut self, venue: Venue) {
        let entry = self.select_mut(venue);
        entry.heartbeat_fail += 1;
    }

    pub fn record_drop(&mut self, venue: Venue) {
        let entry = self.select_mut(venue);
        entry.dropped_events += 1;
    }

    pub fn snapshot(&self, venue: Venue, bar_second: i64) -> ConnectionHealthRow {
        let entry = self.select(venue);
        let now = now_ts_ms();
        let last_age = entry.last_msg_local_ts.map(|ts| (now - ts).max(0));
        ConnectionHealthRow {
            venue,
            bar_second,
            ws_state: entry.ws_state as i64,
            reconnect_count: entry.reconnect_count,
            last_msg_age_ms: last_age,
            heartbeat_sent: Some(entry.heartbeat_sent),
            heartbeat_fail: Some(entry.heartbeat_fail),
            dropped_events: entry.dropped_events,
            notes: None,
        }
    }
}

pub type SharedHealth = Arc<Mutex<HealthState>>;

pub fn new_shared_health() -> SharedHealth {
    Arc::new(Mutex::new(HealthState::new()))
}
