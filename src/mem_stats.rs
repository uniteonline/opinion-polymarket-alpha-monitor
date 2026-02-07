use jemalloc_ctl::{epoch, stats};
use std::time::Duration;
use tracing::{info, warn};

const MEM_STATS_ENV: &str = "MONITOR_MEM_STATS";

pub fn spawn_if_enabled(interval: Duration) {
    if !enabled() {
        return;
    }

    std::thread::spawn(move || loop {
        if let Some(stats) = read_stats() {
            info!(
                "jemalloc_mem allocated={} active={} resident={} mapped={} retained={}",
                stats.allocated, stats.active, stats.resident, stats.mapped, stats.retained
            );
        }
        std::thread::sleep(interval);
    });
}

pub fn snapshot(stage: &str) {
    if !enabled() {
        return;
    }
    if let Some(stats) = read_stats() {
        info!(
            "mem_snapshot stage={} allocated={} active={} resident={} mapped={} retained={}",
            stage, stats.allocated, stats.active, stats.resident, stats.mapped, stats.retained
        );
    }
}

struct MemStats {
    allocated: usize,
    active: usize,
    resident: usize,
    mapped: usize,
    retained: usize,
}

fn read_stats() -> Option<MemStats> {
    if let Err(err) = epoch::advance() {
        warn!("jemalloc_mem epoch advance failed: {:?}", err);
        return None;
    }
    let allocated = match stats::allocated::read() {
        Ok(value) => value,
        Err(err) => {
            warn!("jemalloc_mem read allocated failed: {:?}", err);
            return None;
        }
    };
    let active = match stats::active::read() {
        Ok(value) => value,
        Err(err) => {
            warn!("jemalloc_mem read active failed: {:?}", err);
            return None;
        }
    };
    let resident = match stats::resident::read() {
        Ok(value) => value,
        Err(err) => {
            warn!("jemalloc_mem read resident failed: {:?}", err);
            return None;
        }
    };
    let mapped = match stats::mapped::read() {
        Ok(value) => value,
        Err(err) => {
            warn!("jemalloc_mem read mapped failed: {:?}", err);
            return None;
        }
    };
    let retained = match stats::retained::read() {
        Ok(value) => value,
        Err(err) => {
            warn!("jemalloc_mem read retained failed: {:?}", err);
            return None;
        }
    };
    Some(MemStats {
        allocated,
        active,
        resident,
        mapped,
        retained,
    })
}

fn enabled() -> bool {
    std::env::var(MEM_STATS_ENV).ok().as_deref() == Some("1")
}
