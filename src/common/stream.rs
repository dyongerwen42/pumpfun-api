use std::error::Error;

use futures::StreamExt;
use serde::Serialize;
use solana_client::{
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
    rpc_response::{Response, RpcLogsResponse},
};
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::types::Cluster;
use crate::error;

pub struct Subscription {
    pub task: JoinHandle<()>,
    pub unsubscribe: Box<dyn Fn() + Send>,
}

impl Subscription {
    pub fn new(task: JoinHandle<()>, unsubscribe: Box<dyn Fn() + Send>) -> Self {
        Subscription { task, unsubscribe }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        (self.unsubscribe)();
        self.task.abort();
    }
}

/* ------------------------- BONK-only stream ------------------------- */

const BONK_MINT: &str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";

#[derive(Debug, Clone, Copy, Serialize)]
pub enum BonkEventKind { SwapBuy, SwapSell, Transfer }

#[derive(Debug, Clone, Serialize)]
pub struct BonkEvent {
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub kind: BonkEventKind,
    pub bonk_delta_ui: f64,
    pub counter_asset: Option<String>,  // "SOL" | "USDC" | "OTHER"
    pub counter_delta_ui: Option<f64>,
    pub owners: Vec<String>,
}

async fn fetch_tx_json(
    rpc: &solana_client::nonblocking::rpc_client::RpcClient,
    sig: &str,
) -> Result<Option<solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta>, crate::error::ClientError> {
    let sig = <solana_sdk::signature::Signature as core::str::FromStr>::from_str(sig)
        .map_err(|e| crate::error::ClientError::OtherError(e.to_string()))?;
    let tx = rpc.get_transaction(&sig, solana_transaction_status::UiTransactionEncoding::JsonParsed).await?;
    Ok(tx)
}

fn map_tx_to_bonk_event(
    sig: &str,
    tx: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta
) -> Option<BonkEvent> {
    let meta = tx.transaction.meta.as_ref()?;
    let pre = meta.pre_token_balances.as_ref();
    let post = meta.post_token_balances.as_ref();

    #[derive(Default, Clone)]
    struct Bal { pre: i128, post: i128, mint: String, owner: String, decimals: u8 }
    let mut by_owner_mint: std::collections::HashMap<(String,String), Bal> =
        std::collections::HashMap::new();

    let mut push = |owner: String, mint: String, amount_raw: Option<String>, decimals: u8, is_pre: bool| {
        if let Some(a) = amount_raw {
            if let Ok(base) = a.parse::<i128>() {
                let k = (owner.clone(), mint.clone());
                let e = by_owner_mint.entry(k).or_insert(Bal{ pre:0, post:0, mint, owner, decimals });
                if is_pre { e.pre = base; } else { e.post = base; }
            }
        }
    };

    if let Some(pre) = pre { for b in pre {
        push(
            b.owner.as_ref()?.to_string(),
            b.mint.clone(),
            b.ui_token_amount.as_ref()?.amount.clone(),
            b.ui_token_amount.as_ref()?.decimals,
            true
        );
    }}
    if let Some(post) = post { for b in post {
        push(
            b.owner.as_ref()?.to_string(),
            b.mint.clone(),
            b.ui_token_amount.as_ref()?.amount.clone(),
            b.ui_token_amount.as_ref()?.decimals,
            false
        );
    }}

    let mut bonk_delta: i128 = 0;
    let mut owners = Vec::new();
    let mut counters: Vec<(String, i128, u8)> = Vec::new();

    for ((_owner, mint), bal) in by_owner_mint.into_iter() {
        let delta = bal.post - bal.pre;
        if delta == 0 { continue; }
        if mint == BONK_MINT {
            bonk_delta += delta;
            owners.push(bal.owner);
        } else {
            counters.push((mint, delta, bal.decimals));
        }
    }
    if bonk_delta == 0 { return None; }

    let counter = counters.into_iter().max_by_key(|(_,d,_)| d.abs());

    let (kind, counter_asset, counter_delta_ui) = if let Some((mint, delta, dec)) = counter {
        let buy = bonk_delta > 0 && delta < 0;
        let sell = bonk_delta < 0 && delta > 0;
        let kind = if buy { BonkEventKind::SwapBuy } else if sell { BonkEventKind::SwapSell } else { BonkEventKind::Transfer };

        let sym = match mint.as_str() {
            "So11111111111111111111111111111111111111112" => "SOL",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => "USDC",
            _ => "OTHER",
        }.to_string();
        let ui = (delta as f64) / 10f64.powi(dec as i32);
        (kind, Some(sym), Some(ui))
    } else {
        (BonkEventKind::Transfer, None, None)
    };

    let bonk_ui = (bonk_delta as f64) / 10f64.powi(5); // BONK heeft 5 decimals

    Some(BonkEvent {
        signature: sig.to_string(),
        slot: tx.slot,
        block_time: tx.block_time,
        kind,
        bonk_delta_ui: bonk_ui,
        counter_asset,
        counter_delta_ui,
        owners,
    })
}

pub async fn subscribe_bonk<F>(
    cluster: Cluster,
    commitment: Option<CommitmentConfig>,
    callback: F,
) -> Result<Subscription, error::ClientError>
where
    F: Fn(
            String,
            Option<BonkEvent>,
            Option<Box<dyn Error + Send + Sync>>,
            Response<RpcLogsResponse>,
        ) + Send
        + Sync
        + 'static,
{
    let ws_url = &cluster.rpc.ws;
    let http_url = &cluster.rpc.http;

    let pubsub_client = PubsubClient::new(ws_url)
        .await
        .map_err(error::ClientError::PubsubClientError)?;
    let rpc_http = solana_client::nonblocking::rpc_client::RpcClient::new(http_url.to_string());

    let (tx, _) = mpsc::channel(1);
    let (cb_tx, mut cb_rx) = mpsc::channel(1000);

    tokio::spawn(async move {
        while let Some((sig, event, err, log)) = cb_rx.recv().await {
            callback(sig, event, err, log);
        }
    });

    let task = tokio::spawn(async move {
        // luister alles; filter op BONK via pre/postTokenBalances
        let (mut stream, _unsubscribe) = pubsub_client
            .logs_subscribe(
                RpcTransactionLogsFilter::All,
                RpcTransactionLogsConfig {
                    commitment: Some(commitment.unwrap_or(cluster.commitment)),
                },
            )
            .await
            .unwrap();

        while let Some(log) = stream.next().await {
            let signature = log.value.signature.clone();
            match fetch_tx_json(&rpc_http, &signature).await {
                Ok(Some(full)) => {
                    if let Some(evt) = map_tx_to_bonk_event(&signature, &full) {
                        let _ = cb_tx
                            .send((signature.clone(), Some(evt), None, log.clone()))
                            .await;
                    }
                }
                Ok(None) => {
                    let _ = cb_tx
                        .send((signature.clone(), None, Some("no transaction returned".into()), log.clone()))
                        .await;
                }
                Err(e) => {
                    let _ = cb_tx
                        .send((signature.clone(), None, Some(Box::<dyn Error + Send + Sync>::from(e.to_string())), log.clone()))
                        .await;
                }
            }
        }
    });

    Ok(Subscription::new(
        task,
        Box::new(move || {
            let _ = tx.try_send(());
        }),
    ))
}
