//! Cost readings from Linux: CPU time per thread and memory from `/proc/self`, and network byte
//! counters from `/proc/net/dev` (a whole interface) or an nftables table (the gossip port only).

use std::{collections::HashMap, fs, process::Command};

/// CPU ticks (utime + stime) per thread, and for the whole process.
#[derive(Clone, Debug, Default)]
pub struct CpuSample {
    threads: HashMap<i32, (String, u64)>,
    process: u64,
}

pub fn cpu_sample() -> CpuSample {
    let mut threads = HashMap::new();
    if let Ok(tasks) = fs::read_dir("/proc/self/task") {
        for entry in tasks.flatten() {
            let Ok(tid) = entry.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            // The thread may have exited since the directory was listed.
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            if let Some(sample) = parse_stat(&stat) {
                threads.insert(tid, sample);
            }
        }
    }
    let process = fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|stat| parse_stat(&stat))
        .map_or(0, |(_, ticks)| ticks);
    CpuSample { threads, process }
}

/// `(comm, utime + stime)` from a `/proc/…/stat` line. The name is in parentheses and may
/// contain spaces, so fields are counted from the last `)`.
fn parse_stat(stat: &str) -> Option<(String, u64)> {
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    let name = stat.get(open + 1..close)?.to_string();
    let rest: Vec<&str> = stat.get(close + 1..)?.split_whitespace().collect();
    // rest[0] is field 3 (state); utime is field 14, stime field 15.
    let utime: u64 = rest.get(11)?.parse().ok()?;
    let stime: u64 = rest.get(12)?.parse().ok()?;
    Some((name, utime + stime))
}

fn clk_tck() -> f64 {
    // SAFETY: sysconf has no preconditions.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as f64 } else { 100.0 }
}

/// Process CPU seconds between two samples.
pub fn process_secs(start: &CpuSample, end: &CpuSample) -> f64 {
    end.process.saturating_sub(start.process) as f64 / clk_tck()
}

/// CPU seconds per thread name (trailing digits dropped, so pools add up), largest first.
pub fn secs_by_thread(start: &CpuSample, end: &CpuSample) -> Vec<(String, f64)> {
    let tck = clk_tck();
    let mut groups: HashMap<String, u64> = HashMap::new();
    for (tid, (name, ticks)) in &end.threads {
        let before = start.threads.get(tid).map_or(0, |(_, t)| *t);
        let group = name
            .trim_end_matches(|c: char| c.is_ascii_digit())
            .to_string();
        *groups.entry(group).or_default() += ticks.saturating_sub(before);
    }
    let mut out: Vec<(String, f64)> = groups
        .into_iter()
        .map(|(name, ticks)| (name, ticks as f64 / tck))
        .filter(|(_, secs)| *secs > 0.0)
        .collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1));
    out
}

/// `(current RSS, peak RSS)` in MiB.
pub fn rss_mib() -> (f64, f64) {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kb = |key: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    (kb("VmRSS:") / 1024.0, kb("VmHWM:") / 1024.0)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Bytes {
    pub rx_bytes: u64,
    pub rx_packets: u64,
    pub tx_bytes: u64,
    pub tx_packets: u64,
}

impl Bytes {
    pub fn since(&self, start: &Bytes) -> Bytes {
        Bytes {
            rx_bytes: self.rx_bytes.saturating_sub(start.rx_bytes),
            rx_packets: self.rx_packets.saturating_sub(start.rx_packets),
            tx_bytes: self.tx_bytes.saturating_sub(start.tx_bytes),
            tx_packets: self.tx_packets.saturating_sub(start.tx_packets),
        }
    }
}

/// Counters of one interface. Includes every other process's traffic on that interface.
pub fn iface(name: &str) -> Option<Bytes> {
    let dev = fs::read_to_string("/proc/net/dev").ok()?;
    for line in dev.lines().skip(2) {
        let Some((iface, rest)) = line.split_once(':') else {
            continue;
        };
        if iface.trim() != name {
            continue;
        }
        let v: Vec<u64> = rest
            .split_whitespace()
            .filter_map(|x| x.parse().ok())
            .collect();
        // rx: bytes packets errs drop fifo frame compressed multicast | tx: bytes packets …
        return Some(Bytes {
            rx_bytes: *v.first()?,
            rx_packets: *v.get(1)?,
            tx_bytes: *v.get(8)?,
            tx_packets: *v.get(9)?,
        });
    }
    None
}

/// Named counters `gossip_in` / `gossip_out` in nftables table `inet <table>` (see README).
/// Needs permission to run `nft`; returns `None` otherwise.
pub fn nft(table: &str) -> Option<Bytes> {
    let out = Command::new("nft")
        .args(["-j", "list", "counters", "table", "inet", table])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let mut bytes = Bytes::default();
    let mut found = false;
    for item in json.get("nftables")?.as_array()? {
        let Some(counter) = item.get("counter") else {
            continue;
        };
        let packets = counter.get("packets").and_then(|v| v.as_u64()).unwrap_or(0);
        let total = counter.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0);
        match counter.get("name").and_then(|v| v.as_str()) {
            Some("gossip_in") => {
                bytes.rx_bytes = total;
                bytes.rx_packets = packets;
                found = true;
            }
            Some("gossip_out") => {
                bytes.tx_bytes = total;
                bytes.tx_packets = packets;
                found = true;
            }
            _ => {}
        }
    }
    found.then_some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_line_with_spaces_in_name() {
        let stat = "123 (sol Gossip) S 1 2 3 4 5 6 7 8 9 10 40 2 0 0 20 0 1 0";
        assert_eq!(parse_stat(stat), Some(("sol Gossip".to_string(), 42)));
    }
}
