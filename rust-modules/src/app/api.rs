//! Debug control commands for the remote FIFO and Unix socket.
//! Arm the socket at launch with the instance's plxnative-api dev trigger.

mod socket;

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde_json::json;

use plx_base::eventlog::log;

static PENDING: Mutex<VecDeque<socket::Request>> = Mutex::new(VecDeque::new());

/// Requests waiting for the owning player, session or screenshot code to settle.
static AWAITING: Mutex<VecDeque<InFlight>> = Mutex::new(VecDeque::new());
/// What a parked request is waiting for.
enum What {
    /// `play:<rk>` — answers `play_started` (with the title) on the first frame that
    /// reaches `playing`; `play_failed` on the engine's verdict, a supersession, a
    /// staging failure, or the deadline.
    Play {
        rk: String,
        generation: Option<u32>,
    },
    /// `seek:<ms>` — answers `seek_landed` once the engine has ACTUALLY sought
    /// (`seen_seeking` — it enters `seeking` on the frame `request_seek` runs) AND the
    /// position settles within 3 s of the target; `seek_failed` when the session leaves
    /// playback or on the deadline.
    Seek {
        ms: u64,
        generation: u32,
        seen_seeking: bool,
    },
    Transport {
        play: bool,
        generation: u32,
    },
    /// `profile:<index>` — the switch is `profiles → switching → ready|error`, so the
    /// entry waits OUT the `switching` leg: `ready` is the seated profile (with its
    /// title), `error` is the flow's error line, a return to the roster after the switch
    /// began is a dead pick, and any other phase is the flow leaving without it.
    /// `seen_switching` tells the first still-on-the-roster frames (the command is
    /// processed a frame or two after the pick) from a return to the roster.
    Profile {
        index: usize,
        from: plx_session::auth::Phase,
        seen_switching: bool,
    },
    /// `shot` — answers `shot_captured` when the numbered PNG is written (shot.rs) or
    /// `shot_failed` when the capture fails or on the deadline.
    Shot,
}
/// One parked typed request: the id its answer must echo, and the instant it parked.
struct InFlight {
    reply: socket::ReplyTo,
    since: Instant,
    what: What,
}
/// In-flight horizons: a play that never seats, a seek that never lands, a switch that
/// never settles, a capture that never fires — each answers its `*failed` outcome at the
/// deadline instead of leaving the client hanging.
const AWAIT_PLAY_SECS: u64 = 120;
const AWAIT_SEEK_SECS: u64 = 10;
const AWAIT_TRANSPORT_SECS: u64 = 10;
const AWAIT_PROFILE_SECS: u64 = 60;
const AWAIT_SHOT_SECS: u64 = 10;
/// The parked table is bounded like the command queue: a flooded client cannot park the
/// frame thread with an unbounded table. The overflow answers the OLDEST entry with
/// `queue_full` (its id still has to be echoed exactly once).
const AWAITING_MAX: usize = 16;
/// Park one typed request for its later outcome — no response now; the OUTCOME is the
/// response.
fn arm_awaiting(reply: socket::ReplyTo, what: What) {
    let mut guard = AWAITING.lock().unwrap_or_else(|e| e.into_inner());
    if guard.len() >= AWAITING_MAX {
        let Some(old) = guard.pop_front() else {
            return;
        };
        old.reply
            .send(socket::Reply::err(socket::ApiError::QueueFull));
        log("api: in-flight table full");
    }
    guard.push_back(InFlight {
        reply,
        since: Instant::now(),
        what,
    });
}
/// Every parked play request answers now with the named reason: a stop or a newer play
/// just bumped the play-intent generation, so their rituals can no longer run — the
/// answer is due at the bump, not at the deadline.
fn drop_plays(reason: &str) {
    let mut guard = AWAITING.lock().unwrap_or_else(|e| e.into_inner());
    let mut keep = VecDeque::new();
    while let Some(f) = guard.pop_front() {
        if let What::Play { rk, .. } = f.what {
            f.reply.send(socket::Reply::ok(socket::Outcome::PlayFailed {
                rk,
                reason: reason.to_string(),
            }));
        } else {
            keep.push_back(f);
        }
    }
    *guard = keep;
}
/// Correlate the shared play continuation with the route generation it started.
pub(super) fn play_wait_finished(before: u32) {
    let after = plx_media::route::playback_trace_generation();
    if before == after {
        drop_plays("the item could not be played");
        return;
    }
    let mut waiting = AWAITING.lock().unwrap_or_else(|e| e.into_inner());
    for request in waiting.iter_mut() {
        if let What::Play { generation, .. } = &mut request.what {
            *generation = Some(after);
        }
    }
}

/// A shot capture landed (shot.rs, frame thread): the parked shot read answers with the
/// numbered file. A capture fired by the FIFO (no parked entry) is a no-op.
fn shot_landed(path: String) {
    resolve_shot(socket::Reply::ok(socket::Outcome::ShotCaptured { path }));
}
/// A shot capture failed (shot.rs, frame thread): the parked shot read answers with the
/// reason.
fn shot_failed(reason: String) {
    resolve_shot(socket::Reply::ok(socket::Outcome::ShotFailed { reason }));
}
pub(super) fn shot_completed(result: Result<String, String>) {
    match result {
        Ok(path) => shot_landed(path),
        Err(reason) => shot_failed(reason),
    }
}

/// The shot's answer goes to the OLDEST parked shot read and only to it: a capture is
/// one-shot, and a second parked read that got no capture of its own answers at its
/// deadline.
fn resolve_shot(r: socket::Reply) {
    let mut guard = AWAITING.lock().unwrap_or_else(|e| e.into_inner());
    let pos = guard.iter().position(|f| matches!(f.what, What::Shot));
    if let Some(i) = pos {
        let f = guard.remove(i).expect("position came from the same queue");
        f.reply.send(r);
    }
}
/// The per-frame half of the request→outcome correlation: [`publish`] calls this once
/// per frame — the one place that reads the state, the position, the auth flow and the
/// title together, which is exactly the data every parked request's answer needs. An
/// entry resolves when the frame it waited for has arrived; the response goes over the
/// socket under the entry's id.
unsafe fn resolve_awaiting(app: &mut crate::app::App, st: plx_media::player::PlaybackState) {
    let mut guard = AWAITING.lock().unwrap_or_else(|e| e.into_inner());
    if guard.is_empty() {
        return;
    }
    let api_wait = app
        .menu_play_await
        .as_ref()
        .is_some_and(super::input::MenuPlayAwait::is_api);
    let bridge = app.runtime.bridge(&mut app.bridge);
    let views = bridge.views();
    let ps = &app.player.session;
    let auth = views.auth.0;
    let state_name = state_str(st);
    let pos_ms = plx_media::player::playpos_ns(ps).unwrap_or(0) / 1_000_000;
    let now = Instant::now();
    let mut keep = VecDeque::new();
    while let Some(mut f) = guard.pop_front() {
        let r = match &mut f.what {
            What::Play { rk, generation } => {
                let current = plx_media::route::playback_trace_generation();
                if generation.map_or(!api_wait, |generation| generation != current) {
                    Some(socket::Reply::ok(socket::Outcome::PlayFailed {
                        rk: rk.clone(),
                        reason: "superseded by another playback route".to_string(),
                    }))
                } else if generation.is_some() && st == plx_media::player::PlaybackState::Playing {
                    let title = views
                        .metadata
                        .now_playing()
                        .map(|n| n.title.clone())
                        .unwrap_or_default();
                    Some(socket::Reply::ok(socket::Outcome::PlayStarted {
                        rk: rk.clone(),
                        title,
                    }))
                } else if generation.is_some()
                    && matches!(
                        st,
                        plx_media::player::PlaybackState::Error
                            | plx_media::player::PlaybackState::Idle
                    )
                {
                    let verdict = plx_media::route::play_verdict(ps).map(|s| s.to_string());
                    Some(socket::Reply::ok(socket::Outcome::PlayFailed {
                        rk: rk.clone(),
                        reason: format!(
                            "the engine errored (state {state_name}, verdict {})",
                            verdict.unwrap_or_else(|| "none".to_string())
                        ),
                    }))
                } else if now.duration_since(f.since)
                    > std::time::Duration::from_secs(AWAIT_PLAY_SECS)
                {
                    Some(socket::Reply::ok(socket::Outcome::PlayFailed {
                        rk: rk.clone(),
                        reason: format!(
                            "no first frame within {AWAIT_PLAY_SECS} s (state {state_name})"
                        ),
                    }))
                } else {
                    None
                }
            }
            What::Seek {
                ms,
                generation,
                seen_seeking,
            } => {
                // The engine's own `seeking` state gates the landing: `request_seek` leaves
                // the engine in `seeking` (observable on the frame it runs), so the 3 s
                // position window alone — which could already hold for the target BEFORE any
                // seek runs — answers only for a seek that actually ran and then settled.
                *seen_seeking |= st == plx_media::player::PlaybackState::Seeking;
                if !is_seated(st) || *generation != plx_media::route::playback_trace_generation() {
                    Some(socket::Reply::ok(socket::Outcome::SeekFailed {
                        ms: *ms,
                        reason: format!("the session left playback (state {state_name})"),
                    }))
                } else if st != plx_media::player::PlaybackState::Seeking
                    && seek_landed(*seen_seeking, pos_ms, *ms)
                {
                    Some(socket::Reply::ok(socket::Outcome::SeekLanded {
                        ms: *ms,
                        pos_ms,
                    }))
                } else if now.duration_since(f.since)
                    > std::time::Duration::from_secs(AWAIT_SEEK_SECS)
                {
                    Some(socket::Reply::ok(
                        socket::Outcome::SeekFailed {
                            ms: *ms,
                            reason: format!(
                                "no landing within {AWAIT_SEEK_SECS} s (state {state_name}, pos {pos_ms} ms)"
                            ),
                        },
                    ))
                } else {
                    None
                }
            }
            What::Transport { play, generation } => {
                use plx_media::player::PlaybackState;
                if *generation != plx_media::route::playback_trace_generation() || !is_seated(st) {
                    Some(socket::Reply::err(socket::ApiError::NoEngine {
                        state: state_name,
                    }))
                } else if *play && st == PlaybackState::Playing {
                    Some(socket::Reply::ok(socket::Outcome::Resumed))
                } else if !*play && st == PlaybackState::Paused {
                    Some(socket::Reply::ok(socket::Outcome::Paused))
                } else if now.duration_since(f.since)
                    > std::time::Duration::from_secs(AWAIT_TRANSPORT_SECS)
                {
                    Some(socket::Reply::err(socket::ApiError::Internal {
                        reason: format!("transport did not settle (state {state_name})"),
                    }))
                } else {
                    None
                }
            }
            What::Profile {
                index,
                from,
                seen_switching,
            } => {
                // The switch is `profiles → switching → ready|error`: the `switching`
                // leg is in progress, not a failure.
                *seen_switching =
                    *seen_switching || auth.phase == plx_session::auth::Phase::Switching;
                if auth.phase == plx_session::auth::Phase::Ready {
                    // Seated: the title from the seated profile itself (the roster may
                    // already be cleared), falling back to the picked tile.
                    let title = auth
                        .profile
                        .as_ref()
                        .map(|p| p.title.clone())
                        .or_else(|| auth.users.get(*index).map(|u| u.title.clone()))
                        .unwrap_or_default();
                    Some(socket::Reply::ok(socket::Outcome::ProfileSeated {
                        index: *index,
                        title,
                    }))
                } else if auth.phase == plx_session::auth::Phase::Error {
                    Some(socket::Reply::ok(socket::Outcome::ProfileFailed {
                        index: *index,
                        reason: if auth.error.is_empty() {
                            "the flow reported an error".to_string()
                        } else {
                            auth.error.to_string()
                        },
                    }))
                } else if *seen_switching && auth.phase == plx_session::auth::Phase::Profiles {
                    // The switch began, then went back to the roster without seating (a
                    // denied PIN, a cancel): this pick is dead.
                    Some(socket::Reply::ok(socket::Outcome::ProfileFailed {
                        index: *index,
                        reason: "the flow returned to the roster without seating".to_string(),
                    }))
                } else if !matches!(
                    auth.phase,
                    plx_session::auth::Phase::Profiles | plx_session::auth::Phase::Switching
                ) {
                    // Any other phase (idle, a re-sign-in in flight, erased local state)
                    // is the flow leaving the switch without this pick.
                    Some(socket::Reply::ok(socket::Outcome::ProfileFailed {
                        index: *index,
                        reason: format!("the flow left the switch (phase {:?})", auth.phase)
                            .to_lowercase(),
                    }))
                } else if now.duration_since(f.since)
                    > std::time::Duration::from_secs(AWAIT_PROFILE_SECS)
                {
                    // Still on the roster or still switching after the horizon: the
                    // switch will not land.
                    Some(socket::Reply::ok(
                        socket::Outcome::ProfileFailed {
                            index: *index,
                            reason: format!(
                                "the switch did not land within {AWAIT_PROFILE_SECS} s (from {from:?}, now {:?})",
                                auth.phase
                            )
                            .to_lowercase(),
                        },
                    ))
                } else {
                    None
                }
            }
            What::Shot => {
                if now.duration_since(f.since) > std::time::Duration::from_secs(AWAIT_SHOT_SECS) {
                    Some(socket::Reply::ok(socket::Outcome::ShotFailed {
                        reason: format!("no capture within {AWAIT_SHOT_SECS} s"),
                    }))
                } else {
                    None
                }
            }
        };
        if let Some(r) = r {
            // Every outcome this frame judges is stamped with THIS frame's playback state —
            // the state the result belongs to.
            f.reply.send(r.with_state(state_name));
        } else {
            keep.push_back(f);
        }
    }
    *guard = keep;
}
static UP_SINCE: OnceLock<Instant> = OnceLock::new();

pub(super) fn init() {
    let _ = UP_SINCE.get_or_init(Instant::now);
    socket::start();
}

pub(super) fn take_pending() -> Vec<socket::Request> {
    PENDING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .drain(..)
        .collect()
}

fn push_req(req: socket::Request) -> bool {
    let mut pending = PENDING.lock().unwrap_or_else(|e| e.into_inner());
    if pending.len() >= 64 {
        return false;
    }
    pending.push_back(req);
    true
}

pub(super) fn reject_during_replay(req: socket::Request) {
    req.reply
        .send(socket::Reply::err(socket::ApiError::Internal {
            reason: "the app is replaying recorded input".to_string(),
        }));
}

fn state_file(app: &mut super::App, fr: &super::run::Frame) {
    let json = snapshot(app, fr).to_string();
    let path = plx_base::paths::in_runtime_dir("state.json");
    if plx_base::storage_worker::submit(move || {
        if let Err(error) = plx_plex::plex::session::write_atomic(&path, json.as_bytes()) {
            log(&format!(
                "api: state: could not write {}: {error:?}",
                path.display()
            ));
        }
    })
    .is_err()
    {
        log("api: state: storage worker refused the write");
    }
}

pub(crate) unsafe fn handle(
    app: &mut crate::app::App,
    fr: &mut crate::app::run::Frame,
    spec: &str,
) -> bool {
    let (kind, arg) = match spec.split_once(':') {
        Some(pair) => pair,
        None => (spec, ""),
    };
    match kind {
        "play" => handle_play(app, fr, arg),
        "state" => {
            state_file(app, fr);
            true
        }
        "pause" => handle_transport(app, fr, false),
        "resume" => handle_transport(app, fr, true),
        "stop" => handle_stop(app),
        "seek" => match arg.parse::<u64>().ok().and_then(seek_ns) {
            Some(ns) => app
                .adapters
                .player
                .request_seek(&mut app.player.session, ns),
            _ => false,
        },
        "audio" => handle_track(app, fr, arg, true),
        "sub" => handle_track(app, fr, arg, false),
        "quality" => handle_quality(app, fr, arg),
        "profile" => handle_profile(app, fr, arg),
        _ => false,
    }
}

/// The frame thread's half of a TYPED UDS request: the same handlers the FIFO's tokens
/// reach (the typed surface is a CONVERSATION over the command set, not a second command
/// set), and the response over the socket instead of a log line — the outcome in the
/// action's own vocabulary, or a structured error naming the reason.
pub(crate) unsafe fn execute_request(
    app: &mut crate::app::App,
    fr: &mut crate::app::run::Frame,
    req: socket::Request,
) {
    use socket::{ApiError, Outcome, Reply, Verb};
    let reply = req.reply;
    let st = plx_media::player::state(&app.player.session);
    let st_name = state_str(st);
    let no_engine = Reply::err(ApiError::NoEngine { state: st_name });
    // Every reply this frame produces is stamped with THIS frame's playback state — the
    // state the result belongs to (a reply is a judgment, not an acknowledgement).
    let answer = |r: Reply| reply.send(r.with_state(st_name));
    match req.verb {
        Verb::Play { rk } => {
            // Commit, then park: the response is the OUTCOME — the first playing frame,
            // or the refusal that lands in between (a refusal the frame can already see
            // answers now, the ones only time reveals ride the per-frame scan) — never
            // this acceptance.
            let accepted = handle_play(app, fr, &rk);
            if accepted {
                arm_awaiting(
                    reply.clone(),
                    What::Play {
                        rk,
                        generation: None,
                    },
                );
            } else {
                answer(Reply::ok(Outcome::PlayFailed {
                    rk,
                    reason: "no server or item selected".to_string(),
                }));
            }
        }
        Verb::Seek { ms } => {
            // The seek target is a `i64` of NANOSECONDS: a ms beyond ~292 years is not a
            // seek (and a blind `as i64` would wrap negative — the FIFO's own guard
            // rejects negatives for exactly this reason).
            let ns = seek_ns(ms);
            if is_seated(st) {
                if let Some(ns) = ns {
                    if app
                        .adapters
                        .player
                        .request_seek(&mut app.player.session, ns)
                    {
                        arm_awaiting(
                            reply.clone(),
                            What::Seek {
                                ms,
                                generation: plx_media::route::playback_trace_generation(),
                                seen_seeking: plx_media::player::state(&app.player.session)
                                    == plx_media::player::PlaybackState::Seeking,
                            },
                        );
                    } else {
                        answer(Reply::ok(Outcome::SeekFailed {
                            ms,
                            reason: "seek was refused".into(),
                        }));
                    }
                } else {
                    answer(Reply::err(ApiError::Internal {
                        reason: "the seek target overflows nanoseconds".to_string(),
                    }));
                }
            } else {
                answer(no_engine);
            }
        }
        Verb::Audio { sid } => {
            let ok = handle_track(app, fr, &sid.to_string(), true);
            answer(if ok {
                Reply::ok(Outcome::TrackCommitted {
                    track: "audio",
                    sid,
                })
            } else {
                Reply::err(ApiError::NoTrack { sid })
            });
        }
        Verb::Sub { sid } => {
            let ok = handle_track(app, fr, &sid.to_string(), false);
            answer(if ok {
                Reply::ok(Outcome::TrackCommitted { track: "sub", sid })
            } else {
                Reply::err(ApiError::NoTrack { sid })
            });
        }
        Verb::Quality { rung } => {
            let ok = handle_quality(app, fr, &rung);
            answer(if ok {
                Reply::ok(Outcome::QualityCommitted { rung })
            } else {
                Reply::err(ApiError::UnknownRung { rung })
            });
        }
        Verb::Profile { index } => {
            // Structured pre-checks the FIFO's token cannot express: the session machine
            // silently drops a pick made outside the roster phase — say so — and the
            // roster must have that index.
            let (auth_phase, user_count) = {
                let bridge = app.runtime.bridge(&mut app.bridge);
                let views = bridge.views();
                (views.auth.0.phase, views.auth.0.users.len())
            };
            if auth_phase != plx_session::auth::Phase::Profiles {
                answer(Reply::err(ApiError::NotOnRoster {
                    phase: format!("{auth_phase:?}").to_lowercase(),
                }));
            } else if index >= user_count {
                answer(Reply::err(ApiError::NoProfile {
                    index,
                    count: user_count,
                }));
            } else {
                commit_profile(app, index, None);
                // Parked: the response is the SEATING — the flow reaching `ready` after
                // this pick (with the profile's title) — or the flow's error line, or
                // the flow moving on without the pick.
                arm_awaiting(
                    reply.clone(),
                    What::Profile {
                        index,
                        from: auth_phase,
                        seen_switching: false,
                    },
                );
            }
        }
        Verb::Pause | Verb::Resume => {
            let play = matches!(req.verb, Verb::Resume);
            if handle_transport(app, fr, play) {
                arm_awaiting(
                    reply.clone(),
                    What::Transport {
                        play,
                        generation: plx_media::route::playback_trace_generation(),
                    },
                );
            } else {
                answer(no_engine);
            }
        }
        Verb::Stop => {
            // Idempotent: an idle session has nothing to leave, and "stopped" is still
            // the true answer to "is it playing".
            handle_stop(app);
            answer(Reply::ok(Outcome::Stopped));
        }
        Verb::State => {
            answer(Reply::ok(Outcome::State {
                snapshot: snapshot(app, fr),
            }));
        }
        Verb::Shot => {
            // Parked: the response is the CAPTURE (the numbered PNG, written on the
            // frame swap after this one) or the capture's refusal — not this request.
            crate::shot::request();
            arm_awaiting(reply.clone(), What::Shot);
        }
    }
}

/// The playback state's snapshot vocabulary: the same strings the state file's `state`
/// field and the structured `no_engine` error answer share.
fn state_str(st: plx_media::player::PlaybackState) -> &'static str {
    match st {
        plx_media::player::PlaybackState::Idle => "idle",
        plx_media::player::PlaybackState::Resolving => "resolving",
        plx_media::player::PlaybackState::Connecting => "connecting",
        plx_media::player::PlaybackState::Buffering => "buffering",
        plx_media::player::PlaybackState::Seeking => "seeking",
        plx_media::player::PlaybackState::Playing => "playing",
        plx_media::player::PlaybackState::Paused => "paused",
        plx_media::player::PlaybackState::Error => "error",
    }
}

/// The seated-engine question the transport verbs (and the typed seek) ask before they
/// touch the engine: `sink().pause` and `request_seek` are Starfish calls, and a session
/// without an engine (idle, resolving, connecting, errored) has nothing for them to
/// reach.
fn is_seated(st: plx_media::player::PlaybackState) -> bool {
    matches!(
        st,
        plx_media::player::PlaybackState::Buffering
            | plx_media::player::PlaybackState::Paused
            | plx_media::player::PlaybackState::Seeking
            | plx_media::player::PlaybackState::Playing
    )
}

fn seek_ns(ms: u64) -> Option<i64> {
    i64::try_from(ms).ok()?.checked_mul(1_000_000)
}

/// The `seek_landed` gate: the engine must have actually sought (`seen_seeking` — it
/// passes through `seeking` on the frame `request_seek` runs) AND the position must
/// have settled within 3 s of the target. The position window alone answered a no-op
/// seek on the very next frame, because the target can sit inside the window before
/// any seek runs.
fn seek_landed(seen_seeking: bool, pos_ms: i64, ms: u64) -> bool {
    seen_seeking && pos_ms.abs_diff(ms as i64) <= 3_000
}

unsafe fn handle_play(app: &mut super::App, fr: &super::run::Frame, rk: &str) -> bool {
    let sid = plx_media::route::surface_sid();
    if rk.is_empty() || plx_plex::plex::client_opt().is_none() {
        return false;
    }
    handle_stop(app);
    let mut bridge = app.runtime.bridge(&mut app.bridge);
    bridge.metadata_run(plx_data::stores::metadata::MetadataCmd::RequestDetail {
        sid,
        rk: rk.to_owned(),
    });
    app.menu_play_await = Some(super::input::MenuPlayAwait::for_api(
        sid,
        rk.to_owned(),
        fr.now,
    ));
    true
}

fn handle_transport(app: &mut super::App, fr: &super::run::Frame, play: bool) -> bool {
    if !is_seated(plx_media::player::state(&app.player.session)) {
        return false;
    }
    super::playback::apply_transport(
        &mut app.player.session,
        &mut app.adapters.player,
        Some(play),
        fr.now,
        &mut app.pages,
    )
}

fn handle_stop(app: &mut super::App) -> bool {
    drop_plays("superseded by a stop or a newer play");
    app.menu_play_await = None;
    if plx_media::player::state(&app.player.session) != plx_media::player::PlaybackState::Idle
        || super::bridge::player(&app.pages).is_some()
    {
        super::playback::exit_player(
            &mut app.player,
            &mut app.adapters.player,
            &mut app.repause_at,
            &mut app.refresh_hubs,
            &mut app.pages,
        );
    }
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
    let bridge = app.runtime.bridge(&mut app.bridge);
    let views = bridge.views();
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
        // The track menu's own commit, field for field: the DEMUXER ordinal (the client
        // renderer's input — −1 for an external pick, which the embedded renderer cannot
        // draw), the sidecar the stream names (key + codec — the external pick
        // `player::sidecar` fetches and draws), and no viewer memo (an agent's pick
        // remembers nothing, the ladder's raw commit path).
        let ridx = plx_data::metadata::sub_render_ordinal(&item.subs, i);
        let sidecar = s
            .sidecar_renderable()
            .then(|| (s.key.clone(), s.codec.clone()));
        // The commit itself is infallible (its own docs): the pick is validated above —
        // the sid is one of the seated item's sub rows, so the ordinal is in range — and
        // the sidecar is fetched by `player::sidecar` when the stream loads. So `true`
        // here is the truthful report: "committed", not "confirmed on screen".
        plx_media::route::commit_subtitle_and_sidecar(
            &mut app.player.session,
            ridx,
            s.id,
            sidecar.as_ref().map(|(k, _)| k.clone()),
            sidecar.map(|(_, c)| c).unwrap_or_default(),
            None,
        );
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
    let Ok(q) = serde_json::from_value::<plx_media::route::Quality>(serde_json::Value::String(
        arg.to_string(),
    )) else {
        log(&format!("api: quality: unknown rung {arg}"));
        return false;
    };
    let mut bridge = app.runtime.bridge(&mut app.bridge);
    crate::app::playback::apply_more_action(
        &mut app.player.session,
        &mut app.adapters.player,
        &mut bridge,
        plx_appkit::more_menu::Action::SetQuality(q),
        &mut app.pages.present,
    );
    log(&format!("api: quality: rung set to {arg}"));
    true
}

/// `api:profile:<index>[:<pin>|-]` — commit a roster pick through the session machine\u2019s own
/// command (the same commit the roster\u2019s rows land in). The pick moves the flow to Switching;
/// its outcome is visible in the state file's `auth` section (phase, error).
unsafe fn handle_profile(
    app: &mut crate::app::App,
    _fr: &mut crate::app::run::Frame,
    arg: &str,
) -> bool {
    let Some((idx, pin)) = arg.split_once(':') else {
        return false;
    };
    let Ok(index) = idx.parse::<usize>() else {
        return false;
    };
    let pin = (pin != "-").then(|| pin.to_string());
    commit_profile(app, index, pin);
    true
}

/// The profile commit the FIFO's `api:profile` token and the typed `profile` request both
/// reach: the session machine's own command (the same commit the roster's rows land in).
/// The pick moves the flow to Switching; its outcome is visible in the state file's
/// `auth` section (phase, error).
unsafe fn commit_profile(app: &mut crate::app::App, index: usize, pin: Option<String>) {
    log(&format!(
        "api: profile: select index {index} (pin {})",
        if pin.is_some() { "given" } else { "none" }
    ));
    crate::app::bridge::execute_session_command(
        &mut app.pages,
        plx_session::auth::SessionCmd::SelectProfile { index, pin },
    );
}

pub(super) unsafe fn publish(app: &mut super::App) {
    let st = plx_media::player::state(&app.player.session);
    resolve_awaiting(app, st);
}

fn snapshot(app: &mut super::App, fr: &super::run::Frame) -> serde_json::Value {
    let st = plx_media::player::state(&app.player.session);
    let state_name = state_str(st);
    let bridge = app.runtime.bridge(&mut app.bridge);
    let views = bridge.views();
    let ps = &app.player.session;
    // This frame's auth section: the sign-in flow's phase, the offered "who's watching"
    // roster (title, PIN flag, admin flag) and the flow's error line — the state file's
    // `auth` section.
    let auth = {
        let snap = views.auth.0;
        json!({
            "phase": format!("{:?}", snap.phase).to_lowercase(),
            "error": if snap.error.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(snap.error.to_string())
            },
            "profiles": snap.users.iter().enumerate().map(|(i, u)| {
                json!({
                    "index": i,
                    "title": u.title,
                    "pin": u.protected,
                    "admin": u.admin,
                })
            }).collect::<Vec<_>>(),
        })
    };
    let now = views.metadata.now_playing();
    let playing = views.metadata.playing();
    let tracks = |item: Option<&plx_data::metadata::PlayingItem>,
                  audio: bool|
     -> Vec<serde_json::Value> {
        let Some(item) = item else {
            return Vec::new();
        };
        let list: &Vec<plx_data::metadata::Stream> = if audio { &item.audio } else { &item.subs };
        list.iter()
            .enumerate()
            .map(|(i, s)| {
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
            })
            .collect()
    };
    let verdict = plx_media::route::play_verdict(ps).map(|s| s.to_string());
    let mut url = plx_media::route::url(ps);
    if let Some(query) = url.find('?') {
        url.truncate(query);
    }
    let mut obj = json!({
        "t": fr.now,
        "up_ms": UP_SINCE.get().map(|t| t.elapsed().as_millis() as u64).unwrap_or(0),
        "state": state_name,
        "state_id": st as u8,
        "playing": plx_media::player::is_playing(ps),
        // The viewer's pause — `true` while the transport is paused for the viewer (a claim
        // hold's own pause is NOT the viewer's and stays out of this lens).
        "paused": plx_media::player::transport::viewer_paused(ps),
        "pos_ms": plx_media::player::playpos_ns(ps).unwrap_or(0) / 1_000_000,
        "dur_ms": plx_media::player::duration_ns(ps).unwrap_or(0) / 1_000_000,
        "title": now.map(|n| n.title.clone()).unwrap_or_default(),
        "ep_title": now.map(|n| n.ep_title.clone()).unwrap_or_default(),
        "rk": playing.map(|p| p.rk.clone()).unwrap_or_default(),
        "kind": playing.map(|p| p.kind.clone()).unwrap_or_default(),
        "url": url,
        "transcode": plx_media::route::transcode_session(ps),
        "kbps": plx_media::route::transport_kbps(ps),
        "audio_sid": plx_media::route::cur_audio_sid(ps),
        // The session's live subtitle pick (0 = Off), the read-out an agent verifies a `sub`
        // commit against: `subs[].selected` mirrors the metadata the plan set, which a live
        // commit does not rewrite.
        "sub_sid": plx_media::route::cur_sub_sid(ps),
        "verdict": verdict,
        "audio": tracks(playing, true),
        "subs": tracks(playing, false),
    });
    obj["auth"] = auth;
    obj
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_target_must_fit_signed_nanoseconds() {
        assert_eq!(seek_ns(123), Some(123_000_000));
        assert_eq!(seek_ns(u64::MAX), None);
        assert_eq!(seek_ns(i64::MAX as u64), None);
    }

    /// The `seek_landed` gate: the 3 s position window alone could ALREADY hold for the
    /// target before any seek runs (a target within 3 s of the current position), so a
    /// landing additionally requires the engine to have actually passed through
    /// `seeking`.
    #[test]
    fn seek_landed_requires_a_real_seek() {
        // Inside the window, but no seek ran: not a landing.
        assert!(!seek_landed(false, 1_000, 1_000));
        assert!(!seek_landed(false, 2_900, 0));
        // The seek ran and the position settled inside the window: a landing.
        assert!(seek_landed(true, 2_900, 0));
        assert!(seek_landed(true, 0, 2_900));
        assert!(seek_landed(true, 3_000, 0));
        // The seek ran but the position has not settled inside the window yet.
        assert!(!seek_landed(true, 3_001, 0));
        assert!(!seek_landed(true, 0, 3_001));
    }
}
