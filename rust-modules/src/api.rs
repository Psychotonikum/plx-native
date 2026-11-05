//! The command API: a READ-ONLY local HTTP surface for an agent (an LLM over SSH, a test
//! harness, an operator's script) to read the app's state.
//!
//! Commands travel the FIFO (`/var/palm/jail/…/plxnative-remote`) — the single input path
//! the frame loop drains once per frame inside `run::ingest`. This HTTP surface no longer
//! stages any `api:` token: every write route is gone. What remains are the three reads
//! plus the binary screenshot answer, which need no token queue at all.
//!
//! Everything else in this crate's input story is gated — the remote FIFO is a dev/test
//! channel, the lab command long-poll is a `lab-diagnostics` feature, `capture` needs a
//! devtrigger file. This one is on in the **debug flavour only**: a debug television
//! nobody can reach from a keyboard is also a television nobody can debug, and the debug
//! install (`com.beb.plxnative.debug`) is a developer machine in the first place. In the
//! stable build the module compiles in but `init` never starts the listener, so nothing
//! binds. While it runs it binds `INADDR_ANY` with no credential, the same exposure
//! `capture` accepts when armed: the threat is the LAN, and the surface only exposes
//! playback state plus a screenshot — nothing a remote control could not already see.
//!
//! Architecture, mirroring `capture` (one listener, no threads per connection beyond what
//! `task::spawn` allows, state in statics):
//!
//! - an API thread runs a tiny HTTP/1.1 server on `capture`'s port + 10
//!   (`default_port` below). It parses one request per connection and answers JSON (or a
//!   PNG for `/shot`).
//! - the frame loop republishes the state snapshot every frame via
//!   [`report`](run::report) → `publish` into the `SNAPSHOT` static; `GET /state`
//!   answers from it. No tokens flow into the frame loop from this surface.
//!
//! Wire (all JSON except `/shot`):
//! - `GET /health`  → `{app, flavour, port, up_ms}`
//! - `GET /state`   → the last frame's snapshot, or `{"error":"no frame yet"}`
//! - `GET|POST /shot` — the one binary answer in this API (a PNG, so it bypasses the
//!   JSON-only `reply`): a screenshot of the app's own UI plane — what the app draws (the
//!   home rows, the detail page, the player HUD) — read back from the GL surface by the
//!   frame thread before the swap. It is NOT the video overlay: the overlay lives on a
//!   hardware plane the read-back cannot see (the same limit `capture` documents; the shot
//!   discards the framebuffer's alpha for the same reason — a compositor instruction, not
//!   opacity). 200 `image/png`, or 504 when the frame loop presented nothing within the
//!   deadline.
//!
//! The `api:` verbs, performed on the frame thread by [`handle`] (all of them, by
//! construction, go through the media layer's own commit functions — the same ones the
//! track menu's picks land in — so nothing here re-implements a playback decision). The
//! FIFO is the only door now; the HTTP surface stages none of these.
//! - `api:seek:<ms>`  — `player::request_seek(ms * 1_000_000)`.
//! - `api:audio:<sid>` — the `playing()` item's audio `Stream` whose `id == sid`, through
//!   `route::commit_audio_selection` with the panel's `audio_ordinal` mapping.
//! - `api:sub:<sid>`  — the subtitle `Stream` whose `id == sid`, through
//!   `route::commit_subtitle_selection` (`sub_idx` = its row, client-renderable).
//! - `api:quality:<rung>` — the `…` menu's own quality commit: `apply_more_action` with
//!   `SetQuality` — the routing question re-asked for the stream on screen, which reloads only
//!   when the answer changed (the live switch while the engine runs); a terminal engine gets
//!   the persist-then-fresh-retry path instead, the failure screen's Retry at the new rung.
//!   The rung is a persisted-quality spelling: `auto`, `original`, `1080p_20_mbps`,
//!   `1080p_8_mbps`, `720p_4_mbps`, `720p_2_mbps`, `480p_720_kbps`.
//! - `api:pause` / `api:resume` — the transport key's effect performed directly on the frame
//!   thread: `lifecycle::set_transport_paused` under the PAUSE/PLAY key's `transport_target`,
//!   plus the HUD linger a key press would extend — the same commit the player page's own
//!   PAUSE/PLAY arms land in. They go direct because the bare-key spelling (`pause`/`play`
//!   FIFO tokens) rides SDL key synthesis, which this television build never delivers to the
//!   event loop (measured: the pre-existing devtrigger FIFO is inert the same way).
//! - `api:stop` — the one leave-playback ritual the STOP key performs, by the same direct path:
//!   `playback::exit_player` (supersede the play, close the overlays, stop the buffer feed,
//!   return to the origin page, arm the Continue-Watching refresh).

use std::net::{TcpListener, TcpStream};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::json;

use plx_base::eventlog::log;

/// `capture`'s port band plus ten: 8920 stable / 8921 debug / 8922 nightly. `capture`
/// owns 8910..8912 (its `default_port`'s three-way rule); this offset keeps the two
/// surfaces one rule apart and out of each other's way.
pub(crate) fn default_port() -> u16 {
    crate::capture::default_port() + 10
}

/// The last frame's snapshot, republished by `run::report` into a string the API thread
/// hands back verbatim. `None` until the first frame (the app is booting).
static SNAPSHOT: Mutex<Option<String>> = Mutex::new(None);
static UP_SINCE: OnceLock<Instant> = OnceLock::new();

/// Boot init: spawn the listener. Debug flavour only — see the module doc for the why.
/// The gate lives HERE (not at the call site) so no caller can start it in another
/// flavour: the stable app id and the host build (no app id at all) fall out of
/// `flavour()` and get a logged refusal instead of a bound port.
pub(crate) fn init() {
    let Some(flavour) = plx_base::paths::flavour() else {
        log("api: no TV flavour (host build) — command API not started");
        return;
    };
    if flavour != "debug" {
        log(&format!("api: {flavour} flavour — command API is debug-only, not started"));
        return;
    }
    let port = default_port();
    let listener = plx_base::task::spawn("api-listen", move || api_listen(port));
    match listener {
        Some(_) => log(&format!("api: listening on :{port}")),
        None => log("api: DISABLED — a worker thread was refused"),
    }
}

/// No-op: the HTTP write routes are gone (the FIFO is now the sole command path), so the
/// frame loop's drain of this queue is a permanent empty loop. Kept as a stub so
/// `run::ingest` compiles unchanged.
pub(crate) fn take_pending() -> Vec<String> {
    Vec::new()
}

// ---------------------------------------------------------------------------
// HTTP listener (API thread)
// ---------------------------------------------------------------------------

/// The listener: accept loop, one request per connection (an agent talks one curl at a
/// time), `Connection: close` on every answer. A refused socket or a wedged client costs
/// the request, never the listener.
fn api_listen(port: u16) {
    let Ok(listener) = TcpListener::bind(("0.0.0.0", port)) else {
        log(&format!("api: bind :{port} failed"));
        return;
    };
    log(&format!("api: bound :{port}"));
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        serve(stream, port);
    }
}

fn serve(stream: TcpStream, port: u16) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let Some((method, path, body)) = read_request(&stream) else {
        return;
    };
    // The one binary answer: a PNG, so it cannot ride the JSON-only `reply`.
    if path == "/shot" && (method == "GET" || method == "POST") {
        shot_reply(&stream);
        return;
    }
    let (status, json) = route(&method, &path, &body, port);
    reply(&stream, status, &json);
}

/// `GET|POST /shot` — the one binary answer in this API (a PNG, so it cannot ride the
/// JSON-only `reply`): a screenshot of the app's own UI plane. Arms an on-demand capture
/// on the frame thread and waits for the read-back: 200 `image/png` on a presented frame,
/// 504 when the frame loop produced nothing within the deadline (wedged loop, or a screen
/// that never drew). Debug-only by construction — the whole API is debug-gated in `init`.
fn shot_reply(mut stream: &TcpStream) {
    use std::io::Write;
    let Some(png) = crate::shot::api_capture(Duration::from_secs(8)) else {
        log("api: shot: no frame presented within the deadline");
        let body =
            json!({"error": "no frame presented within 8s (frame loop wedged or screen never drew)"}).to_string();
        let head = format!(
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(body.as_bytes());
        return;
    };
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        png.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&png);
    log(&format!("api: shot: served {} bytes", png.len()));
}

/// Read `METHOD SP PATH SP HTTP/1.1` + headers + body (up to 64 KiB — a command body is
/// a few tokens). Returns `None` on any framing violation; the connection is dropped.
fn read_request(mut stream: &TcpStream) -> Option<(String, String, String)> {
    use std::io::Read;
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    let mut chunk = [0u8; 1024];
    // headers: read until CRLF CRLF, bounded
    let mut header_end: Option<usize> = None;
    while header_end.is_none() && buf.len() < 16 * 1024 {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(p) = find_crlf_crlf(&buf) {
            header_end = Some(p);
        }
    }
    let header_end = header_end?;
    let head = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut it = request_line.split_whitespace();
    let method = it.next()?.to_string();
    let path = it.next()?.to_string();
    let mut content_length = 0usize;
    for line in lines {
        let Some((k, v)) = line.split_once(':') else { continue };
        if k.trim().eq_ignore_ascii_case("content-length") {
            content_length = v.trim().parse().ok()?;
        }
    }
    if content_length > 64 * 1024 {
        return None;
    }
    let mut body = String::from_utf8_lossy(&buf[header_end + 4..]).into_owned();
    while body.len() < content_length {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        body.push_str(&String::from_utf8_lossy(&chunk[..n]));
    }
    body.truncate(content_length);
    Some((method, path, body))
}

fn find_crlf_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn reply(mut stream: &TcpStream, status: u16, body: &str) {
    use std::io::Write;
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body.as_bytes());
}

/// Route one request. The HTTP surface is READ-ONLY: the three answer-from-`SNAPSHOT`
/// endpoints, and 404/405 for everything else. Commands travel the FIFO (the single
/// input path); the API no longer stages any `api:` token, so `body` is never parsed.
/// `body` is kept in the signature so the caller (`serve`) is unchanged.
fn route(method: &str, path: &str, _body: &str, port: u16) -> (u16, String) {
    match (path, method) {
        ("/health", "GET") => {
            let up = UP_SINCE.get().map(|t| t.elapsed().as_millis() as u64).unwrap_or(0);
            (
                200,
                json!({
                    "app": "plx-native",
                    "flavour": plx_base::paths::flavour().unwrap_or("stable"),
                    "port": port,
                    "up_ms": up,
                })
                .to_string(),
            )
        }
        ("/state", "GET") => match SNAPSHOT.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            Some(s) => (200, s),
            None => (404, json!({"error": "no frame yet"}).to_string()),
        },
        _ => {
            if method == "GET" {
                (404, json!({"error": "no such route"}).to_string())
            } else {
                (405, json!({"error": "method not allowed"}).to_string())
            }
        }
    }
}


// ---------------------------------------------------------------------------
// Frame-thread half
// ---------------------------------------------------------------------------

/// Perform an `api:` verb. Called from `run::ingress_token` with the prefix already
/// peeled. Returns whether the frame loop may consider the token dispatched (the
/// passthrough path returns what the dispatcher said; the `api:` verbs answer their
/// own acceptance here).
pub(crate) unsafe fn handle(app: &mut crate::app::App, fr: &mut crate::app::run::Frame, spec: &str) -> bool {
    let (kind, arg) = match spec.split_once(':') {
        Some(pair) => pair,
        None => (spec, ""),
    };
    match kind {
        "play" => handle_play(app, fr, arg),
        "pause" => handle_transport(app, fr, false),
        "resume" => handle_transport(app, fr, true),
        "stop" => handle_stop(app, fr),
        "seek" => match arg.parse::<i64>() {
            Ok(ms) if ms >= 0 => {
                plx_media::player::request_seek(ms * 1_000_000);
                true
            }
            _ => false,
        },
        "audio" => handle_track(app, fr, arg, true),
        "sub" => handle_track(app, fr, arg, false),
        "quality" => handle_quality(app, fr, arg),
        _ => false,
    }
}

/// The headless play ritual, performed on the frame thread: look up the `rk` in the
/// home hub's `PmsMovie` cache, then the same three calls the dev live-play scenario
/// makes in the same order — request the item (which owns the resolve), ask for its
/// detail (the HUD's context), then seat the engine. No HTTP staging: the FIFO token
/// carries the `rk` directly and the PMS data is already resident in the app's
/// `bridge.hubs` snapshot (populated by the home screen's own catalog load), so the
/// frame thread can resolve without the API thread ever fetching it.
unsafe fn handle_play(
    app: &mut crate::app::App,
    _fr: &mut crate::app::run::Frame,
    rk: &str,
) -> bool {
    let snap = app.bridge.hubs_snapshot();
    let view = snap.view();
    let mut pmm: Option<plx_data::pms::PmsMovie> = None;
    for i in 0..view.hub_count() {
        let Some(hub) = view.hub(i) else { continue };
        if let Some(m) = hub.items.iter().find(|m| m.rk == rk) {
            pmm = Some((**m).clone());
            break;
        }
    }
    let Some(pmm) = pmm else {
        log(&format!("api: play rk {rk}: no item in hubs catalog (stale or unknown rk)"));
        return false;
    };
    let ctx = format!("api play rk {rk}");
    let requested = plx_media::route::request_play_movie(
        &mut app.player.session,
        app.bridge.metadata_mut(),
        &pmm,
        &ctx,
    );
    if requested {
        app.bridge.metadata_mut().run(
            plx_data::stores::metadata::MetadataCmd::RequestDetail {
                sid: pmm.sid,
                rk: pmm.rk.clone(),
            },
        );
        crate::app::playback::start_playback(
            &mut app.player.session,
            &mut app.adapters.player,
            0,
            crate::app::playback::Origin::Here,
            plx_screens::player::input::HUD_HEADLESS_MS,
            None,
            &mut app.pages,
            &mut app.bridge,
        );
    }
    requested
}

/// `api:pause` / `api:resume` — the transport key's effect, performed directly on the frame
/// thread (the bare-key spelling rides SDL synthesis, which this TV build does not deliver —
/// see the module doc). `play` is the key's `Transport` payload: `true` = the PLAY key
/// (resume), `false` = the PAUSE key. Seated-engine guard: `sink().pause` is a Starfish call
/// and a session with no engine (idle, resolving, connecting, errored) has nothing for it to
/// reach — those answer a log line and a false, never a guess at an engine.
unsafe fn handle_transport(
    app: &mut crate::app::App,
    fr: &mut crate::app::run::Frame,
    play: bool,
) -> bool {
    let verb = if play { "resume" } else { "pause" };
    let st = plx_media::player::state(&app.player.session);
    if !matches!(
        st,
        plx_media::player::PlaybackState::Buffering
            | plx_media::player::PlaybackState::Seeking
            | plx_media::player::PlaybackState::Playing
    ) {
        log(&format!("api: {verb}: no seated engine (state {st:?})"));
        return false;
    }
    let target = plx_media::player::lifecycle::transport_target(
        Some(play),
        plx_media::player::lifecycle::viewer_paused(),
    );
    let ok = plx_media::player::lifecycle::set_transport_paused(&mut app.adapters.player, target);
    if !ok {
        log(&format!("api: {verb}: transport commit refused"));
    }
    if let Some(player) = crate::app::bridge::player_mut(&mut app.pages) {
        player.hud.extend(fr.now, plx_screens::player::input::HUD_LINGER_MS);
        player.publish();
    }
    ok
}

/// `api:stop` — the one leave-playback ritual the STOP key performs, by the same direct path:
/// `playback::exit_player` (supersede the play, close the overlays, stop the buffer feed,
/// return to the origin page, arm the Continue-Watching refresh). An idle session has nothing
/// to leave.
unsafe fn handle_stop(app: &mut crate::app::App, _fr: &mut crate::app::run::Frame) -> bool {
    let st = plx_media::player::state(&app.player.session);
    if matches!(st, plx_media::player::PlaybackState::Idle) {
        log("api: stop: nothing to leave (state Idle)");
        return false;
    }
    crate::app::playback::exit_player(
        &mut app.player.session,
        &mut app.adapters.player,
        &mut app.refresh_hubs,
        &mut app.pages,
    );
    true
}

/// `api:audio:<sid>` / `api:sub:<sid>` — find the `playing()` item's track row by PMS
/// stream id and commit it through the media layer's own selection functions (the bare
/// `_selection` commits the ladder uses: no viewer memo is remembered, which is right
/// for an agent).
unsafe fn handle_track(
    app: &mut crate::app::App,
    _fr: &mut crate::app::run::Frame,
    arg: &str,
    audio: bool,
) -> bool {
    let Ok(sid) = arg.parse::<i64>() else {
        return false;
    };
    let views = app.bridge.views();
    let Some(item) = views.metadata.playing() else {
        return false;
    };
    if audio {
        let Some(i) = item.audio.iter().position(|s| s.id == sid) else {
            return false;
        };
        let ord = plx_data::metadata::audio_ordinal(&item.audio, i);
        plx_media::route::commit_audio_selection(
            &mut app.player.session,
            plx_media::route::CarriedAudio::from_stream(&item.audio[i], ord),
        )
    } else {
        let Some(i) = item.subs.iter().position(|s| s.id == sid) else {
            return false;
        };
        let s = &item.subs[i];
        // A picked track is client-renderable by definition (an embedded demux ordinal
        // or a sidecar the client draws); `Off` is the only non-renderable pick and it
        // has no sid to arrive here with.
        plx_media::route::commit_subtitle_selection(&mut app.player.session, i as i32, s.id, true);
        true
    }
}

/// `api:quality:<rung>` — the \u2026 menu\u2019s own quality commit, performed through the menu\u2019s own
/// function: `apply_more_action` re-asks the routing question for the stream on screen and
/// reloads only if the answer changed (the live switch while the engine runs); a terminal
/// engine gets the persist-then-fresh-retry path instead — the failure screen\u2019s Retry at the
/// new rung.
unsafe fn handle_quality(
    app: &mut crate::app::App,
    _fr: &mut crate::app::run::Frame,
    arg: &str,
) -> bool {
    let Ok(q) = serde_json::from_value::<plx_media::route::Quality>(serde_json::Value::String(arg.to_string())) else {
        log(&format!("api: quality: unknown rung {arg}"));
        return false;
    };
    crate::app::playback::apply_more_action(
        &mut app.player.session,
        &mut app.adapters.player,
        &mut app.bridge,
        plx_appkit::more_menu::Action::SetQuality(q),
    );
    log(&format!("api: quality: rung set to {arg}"));
    true
}

/// Republish the snapshot, called from `run::report` once per frame. The string form is
/// what `GET /state` answers verbatim; building it here (frame thread) means the API
/// thread never borrows the app.
pub(crate) unsafe fn publish(app: &mut crate::app::App, fr: &mut crate::app::run::Frame) {
    let ps = &app.player.session;
    let st = plx_media::player::state(ps);
    let state_name = match st {
        plx_media::player::PlaybackState::Idle => "idle",
        plx_media::player::PlaybackState::Resolving => "resolving",
        plx_media::player::PlaybackState::Connecting => "connecting",
        plx_media::player::PlaybackState::Buffering => "buffering",
        plx_media::player::PlaybackState::Seeking => "seeking",
        plx_media::player::PlaybackState::Playing => "playing",
        plx_media::player::PlaybackState::Paused => "paused",
        plx_media::player::PlaybackState::Error => "error",
    };
    let views = app.bridge.views();
    let now = views.metadata.now_playing();
    let playing = views.metadata.playing();
    let tracks = |item: Option<&plx_data::metadata::PlayingItem>, audio: bool| -> Vec<serde_json::Value> {
        let Some(item) = item else {
            return Vec::new();
        };
        let list: &Vec<plx_data::metadata::Stream> = if audio { &item.audio } else { &item.subs };
        list.iter().enumerate().map(|(i, s)| {
            let mut o = json!({
                "sid": s.id,
                "ord": plx_data::metadata::audio_ordinal(&list, i),
                "codec": s.codec,
                "lang": s.lang,
                "title": s.title,
                "selected": s.selected,
            });
            if audio {
                o["ch"] = json!(s.channels);
                o["layout"] = json!(s.layout);
                o["kbits"] = json!(s.bitrate);
                o["default"] = json!(s.default);
            } else {
                o["external"] = json!(s.external);
                o["forced"] = json!(s.forced);
            }
            o
        }).collect()
    };
    let verdict = plx_media::route::play_verdict(ps).map(|s| s.to_string());
    let obj = json!({
        "t": fr.now,
        "state": state_name,
        "state_id": st as u8,
        "playing": plx_media::player::is_playing(ps),
        // The viewer's pause — `true` while the transport is paused for the viewer (a claim
        // hold's own pause is NOT the viewer's and stays out of this lens).
        "paused": plx_media::player::lifecycle::viewer_paused(),
        "pos_ms": plx_media::player::playpos_ns() / 1_000_000,
        "dur_ms": plx_media::player::duration_ns() / 1_000_000,
        "title": now.map(|n| n.title.clone()).unwrap_or_default(),
        "ep_title": now.map(|n| n.ep_title.clone()).unwrap_or_default(),
        "rk": playing.map(|p| p.rk.clone()).unwrap_or_default(),
        "kind": playing.map(|p| p.kind.clone()).unwrap_or_default(),
        "url": plx_media::route::url(ps),
        "transcode": plx_media::route::transcode_session(ps),
        "kbps": plx_media::route::transport_kbps(ps),
        "audio_sid": plx_media::route::cur_audio_sid(ps),
        "verdict": verdict,
        "audio": tracks(playing, true),
        "subs": tracks(playing, false),
    });
    *SNAPSHOT.lock().unwrap_or_else(|e| e.into_inner()) = Some(obj.to_string());
    let _ = UP_SINCE.get_or_init(Instant::now);
}
