use std::convert::Infallible;
use std::env;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

const MAX_LINE_BYTES: usize = 1024 * 1024;
const CHANNEL_CAPACITY: usize = 256;

#[derive(Clone)]
struct AppState {
    events: broadcast::Sender<Value>,
    token: Arc<String>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let token = env::var("XENOMIT_STREAM_TOKEN").map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "XENOMIT_STREAM_TOKEN must be configured",
        )
    })?;
    if token.is_empty() || token.contains(['\r', '\n']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "XENOMIT_STREAM_TOKEN must be non-empty and contain no newlines",
        ));
    }
    let http_addr = bind_address("XENOMIT_STREAM_HTTP_ADDR", "127.0.0.1:8090")?;
    let ingest_addr = bind_address("XENOMIT_STREAM_INGEST_ADDR", "127.0.0.1:8091")?;
    let http = TcpListener::bind(http_addr).await?;
    let ingest = TcpListener::bind(ingest_addr).await?;
    let (events, _) = broadcast::channel(CHANNEL_CAPACITY);
    let state = AppState {
        events,
        token: Arc::new(token),
    };
    eprintln!("HTTP stream listening on {}", http.local_addr()?);
    eprintln!("botd ingest listening on {}", ingest.local_addr()?);

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/stream", get(stream))
        .with_state(state.clone());
    let http_server = axum::serve(http, app);
    tokio::select! {
        result = http_server => result,
        result = accept_ingest(ingest, state) => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

fn bind_address(key: &str, fallback: &str) -> io::Result<SocketAddr> {
    env::var(key)
        .unwrap_or_else(|_| fallback.to_owned())
        .parse()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, format!("{key}: {error}")))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn stream(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let updates = BroadcastStream::new(state.events.subscribe()).filter_map(|event| {
        match event {
            Ok(value) => Some(Ok::<Event, Infallible>(
                Event::default().event("bot.update").data(value.to_string()),
            )),
            // A slow consumer may miss intermediate status frames. The next
            // event is a complete status update, so it resynchronizes itself.
            Err(_) => None,
        }
    });
    Sse::new(updates)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|candidate| candidate == token)
}

async fn accept_ingest(listener: TcpListener, state: AppState) -> io::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = ingest_connection(socket, state).await {
                eprintln!("ingest connection {peer} closed: {error}");
            }
        });
    }
}

async fn ingest_connection(socket: TcpStream, state: AppState) -> io::Result<()> {
    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    if !read_bounded_line(&mut reader, &mut line).await? {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed before AUTH",
        ));
    }
    let presented = line
        .strip_suffix('\n')
        .unwrap_or(&line)
        .strip_suffix('\r')
        .unwrap_or(line.strip_suffix('\n').unwrap_or(&line));
    let Some(presented) = presented.strip_prefix("AUTH ") else {
        write.write_all(b"ERR auth_required\n").await?;
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "missing AUTH",
        ));
    };
    if presented != state.token.as_str() {
        write.write_all(b"ERR unauthorized\n").await?;
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "bad token"));
    }
    write.write_all(b"OK\n").await?;

    loop {
        line.clear();
        if !read_bounded_line(&mut reader, &mut line).await? {
            return Ok(());
        }
        let value: Value = match serde_json::from_str::<Value>(line.trim_end()) {
            Ok(value) if value.is_object() => value,
            _ => continue,
        };
        let _ = state.events.send(value);
    }
}

async fn read_bounded_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut String,
) -> io::Result<bool> {
    let mut bytes = Vec::new();
    let mut limited = reader.take((MAX_LINE_BYTES + 1) as u64);
    let read = limited.read_until(b'\n', &mut bytes).await?;
    if read == 0 {
        return Ok(false);
    }
    if bytes.len() > MAX_LINE_BYTES || !bytes.ends_with(b"\n") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stream frame exceeds 1 MiB or has no line terminator",
        ));
    }
    *line = String::from_utf8(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(true)
}
