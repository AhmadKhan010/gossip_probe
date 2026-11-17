//! The probe's logger. `solana-metrics` logs every datapoint it is given (`datapoint: name,tags
//! field=1i …`), so catching those lines gives us gossip's own counters (packets, CRDS values
//! accepted and rejected per type) without an InfluxDB. Every other log line goes to stderr.

use {
    log::{Level, LevelFilter, Log, Metadata, Record},
    std::{collections::BTreeMap, sync::Mutex},
};

/// Running totals of one datapoint field. Counter fields are reset by gossip on every report, so
/// `sum` is the total; gauge fields (table size, node count) only make sense as `last`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Agg {
    pub sum: f64,
    pub last: f64,
    pub n: u64,
}

/// `"measurement.field"` → totals.
pub type Table = BTreeMap<String, Agg>;

static TABLE: Mutex<Table> = Mutex::new(BTreeMap::new());

struct ProbeLogger {
    print: LevelFilter,
}

impl Log for ProbeLogger {
    // Info must be enabled: `datapoint_info!` only submits when `log_enabled!(Info)`.
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if record.target().starts_with("solana_metrics") {
            let line = record.args().to_string();
            if let Some(point) = line.strip_prefix("datapoint: ") {
                add(point);
                return;
            }
        }
        if record.level() <= self.print {
            eprintln!("[{} {}] {}", record.level(), record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

/// Installs the logger. `verbose` also prints warnings from gossip; errors are always printed.
pub fn install(verbose: bool) -> Result<(), log::SetLoggerError> {
    let print = if verbose {
        LevelFilter::Warn
    } else {
        LevelFilter::Error
    };
    log::set_boxed_logger(Box::new(ProbeLogger { print }))?;
    log::set_max_level(LevelFilter::Info);
    Ok(())
}

pub fn snapshot() -> Table {
    TABLE.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Sum of every field named `field`, in any measurement.
pub fn sum_field(table: &Table, field: &str) -> f64 {
    table
        .iter()
        .filter(|(key, _)| key.rsplit_once('.').is_some_and(|(_, f)| f == field))
        .map(|(_, agg)| agg.sum)
        .sum()
}

/// Last value of a gauge field, from whichever measurement reports it.
pub fn last_field(table: &Table, field: &str) -> Option<f64> {
    table
        .iter()
        .find(|(key, _)| key.rsplit_once('.').is_some_and(|(_, f)| f == field))
        .map(|(_, agg)| agg.last)
}

/// `(type, push, pull)` totals of one CRDS stats measurement, without the `all-` rows.
pub fn crds_by_type(table: &Table, measurement: &str) -> Vec<(String, f64, f64)> {
    let prefix = format!("{measurement}.");
    let mut types: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    for (key, agg) in table {
        let Some(field) = key.strip_prefix(&prefix) else {
            continue;
        };
        let Some((kind, route)) = field.rsplit_once('-') else {
            continue;
        };
        if kind == "all" {
            continue;
        }
        let entry = types.entry(kind.to_string()).or_default();
        match route {
            "push" => entry.0 += agg.sum,
            "pull" => entry.1 += agg.sum,
            _ => {}
        }
    }
    types
        .into_iter()
        .map(|(kind, (push, pull))| (kind, push, pull))
        .collect()
}

fn add(point: &str) {
    let mut tokens = split_unquoted(point).into_iter();
    let Some(head) = tokens.next() else {
        return;
    };
    // `name,tag=value,…`: tags are dropped.
    let name = head.split(',').next().unwrap_or(head);
    let mut table = TABLE.lock().unwrap_or_else(|e| e.into_inner());
    for token in tokens {
        let Some((field, value)) = token.split_once('=') else {
            continue;
        };
        // Integers end in `i`; strings and bools don't parse and are skipped.
        let Ok(value) = value.trim_end_matches('i').parse::<f64>() else {
            continue;
        };
        let agg = table.entry(format!("{name}.{field}")).or_default();
        agg.sum += value;
        agg.last = value;
        agg.n += 1;
    }
}

/// Splits on spaces that are not inside a quoted string field.
fn split_unquoted(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        match c {
            '"' if !escaped => quoted = !quoted,
            ' ' if !quoted => {
                if i > start {
                    out.push(&s[start..i]);
                }
                start = i + 1;
            }
            _ => {}
        }
        escaped = c == '\\';
    }
    if start < s.len() {
        out.push(&s[start..]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_around_quoted_strings() {
        assert_eq!(
            split_unquoted(r#"m,host=a x=1i s="a b" y=2.5"#),
            ["m,host=a", "x=1i", r#"s="a b""#, "y=2.5"]
        );
    }

    #[test]
    fn crds_rows_group_by_type() {
        let mut table = Table::new();
        for (key, sum) in [
            ("s.ContactInfo-push", 3.0),
            ("s.ContactInfo-pull", 4.0),
            ("s.all-push", 9.0),
        ] {
            table.insert(
                key.into(),
                Agg {
                    sum,
                    last: sum,
                    n: 1,
                },
            );
        }
        assert_eq!(
            crds_by_type(&table, "s"),
            [("ContactInfo".to_string(), 3.0, 4.0)]
        );
    }
}
