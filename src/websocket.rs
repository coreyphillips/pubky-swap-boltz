//! Bounded Boltz subscriptions replay the latest persisted state on every reconnect.

use crate::{
    http::{parse_id, AppState},
    model::SwapUpdate,
    Error,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc, OwnedSemaphorePermit},
    task::JoinHandle,
};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    op: String,
    channel: String,
    args: Vec<String>,
}

pub(crate) async fn upgrade(
    State(state): State<AppState>,
    upgrade: WebSocketUpgrade,
) -> crate::Result<Response> {
    let permit = state
        .sockets
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Busy)?;
    Ok(upgrade
        .max_message_size(8192)
        .max_frame_size(8192)
        .on_upgrade(move |socket| serve(socket, state.bridge, permit)))
}

async fn serve(mut socket: WebSocket, bridge: Arc<crate::Bridge>, _permit: OwnedSemaphorePermit) {
    let mut events = bridge.subscribe();
    let mut subscriptions = HashSet::new();
    let mut snapshots = SnapshotWorker::new(bridge.clone());
    loop {
        tokio::select! {
            incoming = socket.recv()=>{
                match incoming {
                    Some(Ok(Message::Text(text)))=>{
                        let Ok(command) = serde_json::from_str::<Command>(&text) else { if !send(&mut socket,json!({"event":"error","error":"invalid subscription"})).await { break; } continue; };
                        if !command_reply(&mut socket,&bridge,&mut subscriptions,&snapshots.requests,command).await { break; }
                    }
                    Some(Ok(Message::Ping(bytes)))=>{ if !matches!(tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Pong(bytes))).await,Ok(Ok(()))) { break; } }
                    Some(Ok(Message::Pong(_)))=>{},
                    _=>break,
                }
            }
            event = events.recv()=>{
                let keep_open = match event {
                    Ok(update) if subscriptions.contains(&update.id)=>queue_snapshots(&mut socket,&snapshots.requests,vec![update.id]).await,
                    Err(broadcast::error::RecvError::Lagged(_))=>queue_snapshots(&mut socket,&snapshots.requests,subscriptions.iter().copied().collect()).await,
                    Err(broadcast::error::RecvError::Closed)=>false,
                    _=>true,
                };
                if !keep_open { break; }
            }
            result = snapshots.results.recv()=>{
                let Some(result) = result else { break; };
                if subscriptions.contains(&result.id) && !send_snapshot(&mut socket,result.update).await { break; }
            }
        }
    }
}

async fn command_reply(
    socket: &mut WebSocket,
    bridge: &crate::Bridge,
    subscriptions: &mut HashSet<Uuid>,
    requests: &mpsc::Sender<Vec<Uuid>>,
    command: Command,
) -> bool {
    if command.channel != "swap.update"
        || !matches!(command.op.as_str(), "subscribe" | "unsubscribe")
        || command.args.len() > 100
    {
        return send(
            socket,
            json!({"event":"error","error":"unsupported subscription"}),
        )
        .await;
    }
    let ids: crate::Result<Vec<_>> = command.args.iter().map(|id| parse_id(id)).collect();
    let Ok(ids) = ids else {
        return send(socket, json!({"event":"error","error":"swap not found"})).await;
    };
    if ids.iter().any(|id| bridge.cached_update(*id).is_err()) {
        return send(socket, json!({"event":"error","error":"swap not found"})).await;
    }
    let mut updated = subscriptions.clone();
    for id in &ids {
        if command.op == "subscribe" {
            updated.insert(*id);
        } else {
            updated.remove(id);
        }
    }
    if updated.len() > 100 {
        return send(
            socket,
            json!({"event":"error","error":"subscription limit reached"}),
        )
        .await;
    }
    *subscriptions = updated;
    if !send(
        socket,
        json!({"event":command.op,"channel":"swap.update","args":command.args}),
    )
    .await
    {
        return false;
    }
    if command.op == "subscribe" {
        queue_snapshots(
            socket,
            requests,
            ids.into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect(),
        )
        .await
    } else {
        true
    }
}

struct SnapshotResult {
    id: Uuid,
    update: crate::Result<SwapUpdate>,
}

struct SnapshotWorker {
    requests: mpsc::Sender<Vec<Uuid>>,
    results: mpsc::Receiver<SnapshotResult>,
    task: JoinHandle<()>,
}

impl SnapshotWorker {
    fn new(bridge: Arc<crate::Bridge>) -> Self {
        let (requests, pending) = mpsc::channel(8);
        let (completed, results) = mpsc::channel(4);
        let task = tokio::spawn(refresh_snapshots(bridge, pending, completed));
        Self {
            requests,
            results,
            task,
        }
    }
}

impl Drop for SnapshotWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn refresh_snapshots(
    bridge: Arc<crate::Bridge>,
    mut requests: mpsc::Receiver<Vec<Uuid>>,
    results: mpsc::Sender<SnapshotResult>,
) {
    while let Some(ids) = requests.recv().await {
        for id in ids {
            let update = bridge.refresh(id).await;
            if results.send(SnapshotResult { id, update }).await.is_err() {
                return;
            }
        }
    }
}

async fn queue_snapshots(
    socket: &mut WebSocket,
    requests: &mpsc::Sender<Vec<Uuid>>,
    ids: Vec<Uuid>,
) -> bool {
    if ids.is_empty() || requests.try_send(ids).is_ok() {
        return true;
    }
    let _ = send(
        socket,
        json!({"event":"error","error":"snapshot capacity reached; reconnect and resubscribe"}),
    )
    .await;
    false
}

async fn send_snapshot(socket: &mut WebSocket, update: crate::Result<SwapUpdate>) -> bool {
    let response = match update {
        Ok(update) => json!({"event":"update","channel":"swap.update","args":[update]}),
        Err(_) => json!({"event":"error","error":"fresh swap status unavailable"}),
    };
    send(socket, response).await
}

async fn send(socket: &mut WebSocket, value: Value) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            socket.send(Message::Text(value.to_string().into()))
        )
        .await,
        Ok(Ok(()))
    )
}
