//! The one RPC step: the identities in the current and next epoch's leader schedule. Those are the
//! nodes whose contact info the sidecar needs.

use {
    anyhow::{Context, Result, anyhow},
    solana_pubkey::Pubkey,
    solana_rpc_client::rpc_client::RpcClient,
    std::{collections::HashSet, str::FromStr},
};

pub struct Leaders {
    pub epoch: u64,
    pub pubkeys: HashSet<Pubkey>,
}

pub fn fetch(rpc_url: &str) -> Result<Leaders> {
    let rpc = RpcClient::new(rpc_url.to_string());
    let info = rpc.get_epoch_info().context("getEpochInfo")?;
    let epoch_start = info.absolute_slot - info.slot_index;
    let mut pubkeys = HashSet::new();
    for first_slot in [epoch_start, epoch_start + info.slots_in_epoch] {
        let schedule = rpc
            .get_leader_schedule(Some(first_slot))
            .with_context(|| format!("getLeaderSchedule({first_slot})"))?;
        let Some(schedule) = schedule else {
            eprintln!("no leader schedule for slot {first_slot}; continuing without it");
            continue;
        };
        for identity in schedule.keys() {
            let pubkey =
                Pubkey::from_str(identity).map_err(|e| anyhow!("leader {identity}: {e:?}"))?;
            pubkeys.insert(pubkey);
        }
    }
    if pubkeys.is_empty() {
        return Err(anyhow!("leader schedule is empty"));
    }
    Ok(Leaders {
        epoch: info.epoch,
        pubkeys,
    })
}
