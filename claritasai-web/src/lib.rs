use axum::{routing::{get, post}, Router, response::{Html, IntoResponse}};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Default)]
pub struct WebState {}

pub fn router(_state: Arc<WebState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/chat", get(chat_get).post(chat_post))
        .route("/chat/stream", get(chat_stream))
}

async fn health() -> &'static str { "OK" }

async fn chat_get() -> Html<&'static str> {
    Html(r##"<!doctype html>
<html><head><meta charset='utf-8'><title>ClaritasAI Chat</title>
<script src="https://unpkg.com/htmx.org@1.9.12"></script></head>
<body>
<h1>ClaritasAI</h1>
<form hx-post="/chat" hx-target="#out" hx-swap="beforeend">
  <input type="text" name="objective" placeholder="Enter objective" style="width:60%" />
  <button type="submit">Run</button>
  <div id="out" style="margin-top:1rem;"></div>
</form>
<div id="stream" hx-get="/chat/stream" hx-trigger="load" hx-swap="beforeend"></div>
</body></html>"##)
}

async fn chat_post() -> impl IntoResponse {
    Html("<div>Received objective. Planning… (stub)</div>")
}

async fn chat_stream() -> impl IntoResponse {
    // Simple polling fallback that returns a small HTML chunk
    let mut body = String::new();
    for i in 1..=5 {
        body.push_str(&format!("<div>event {}</div>", i));
        // simulate latency
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Html(body)
}
