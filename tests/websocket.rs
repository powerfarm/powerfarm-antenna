//! Journey C: WebSocket message -> Receipt -> Capability -> correlated response
//! -> the SAME WebSocket.

mod common;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn message_is_receipted_and_answered_on_the_same_socket() {
    let h = common::start("ws", &[]).await;

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", h.addr))
        .await.expect("connect");

    ws.send(Message::Text(json!({ "id": "client-1", "hello": "antenna" }).to_string().into()))
        .await.unwrap();

    let reply = next_json(&mut ws).await;

    assert_eq!(reply["status"], "completed");
    // The client's own id comes back, so it can correlate without knowing
    // anything about Antenna's internal ids.
    assert_eq!(reply["id"], "client-1");
    assert!(reply["correlation_id"].is_string());
    assert_eq!(reply["result"]["capability"], "echo");
    assert_eq!(reply["result"]["payload"]["hello"], "antenna");

    // The ephemeral socket is gone from the record's point of view; the
    // durable receipt is not.
    let id = reply["receipt_id"].as_str().unwrap();
    let one: serde_json::Value = common::client()
        .get(format!("{}/receipts/{id}", h.base))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(one["receipt"]["transport"], "websocket");
    assert_eq!(one["receipt"]["interaction"], "stream");
    assert_eq!(one["receipt"]["status"], "routed");
}

#[tokio::test]
async fn many_messages_stay_correlated() {
    let h = common::start("ws-many", &[]).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", h.addr))
        .await.unwrap();

    for i in 0..5 {
        ws.send(Message::Text(json!({ "id": i, "n": i }).to_string().into())).await.unwrap();
    }

    // Capabilities run concurrently, so replies may interleave. Correlation is
    // what makes that safe — collect by id rather than assuming order.
    let mut seen = std::collections::HashMap::new();
    for _ in 0..5 {
        let r = next_json(&mut ws).await;
        let id = r["id"].as_i64().expect("client id echoed back");
        seen.insert(id, r["result"]["payload"]["n"].as_i64().unwrap());
    }
    for i in 0..5 {
        assert_eq!(seen.get(&i), Some(&i), "reply {i} must carry its own payload back");
    }
}

#[tokio::test]
async fn transport_noise_creates_no_receipts() {
    let h = common::start("ws-noise", &[]).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", h.addr))
        .await.unwrap();

    ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
    ws.send(Message::Text(json!({ "real": true }).to_string().into())).await.unwrap();

    // Drain until the real answer arrives; pongs are not receipts.
    loop {
        let msg = ws.next().await.unwrap().unwrap();
        if let Message::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["result"]["payload"]["real"], true);
            break;
        }
    }

    let list: serde_json::Value = common::client()
        .get(format!("{}/receipts?limit=50", h.base))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(list["count"], 1, "ping/pong are transport mechanics, not received signals");
}

async fn next_json<S>(ws: &mut S) -> serde_json::Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match ws.next().await.expect("stream open").expect("no ws error") {
            Message::Text(t) => return serde_json::from_str(&t).expect("json reply"),
            _ => continue,
        }
    }
}
