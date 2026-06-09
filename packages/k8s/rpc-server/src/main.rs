// In-pod RPC server. Port of src/k8s/rpc-server.py to Rust so the
// in-pod helper has no runtime dependency on the container image
// (no python3, no node — just a static musl binary).

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::os::unix::io::FromRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tiny_http::{Header, Method, Request, Response, Server};

const LOG_DIR: &str = "/tmp/rpc-logs";
const PORT_FILE: &str = "/tmp/rpc-server.port";
const MAX_CHUNK: usize = 1_048_576;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);
const KILL_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JobStatus {
    Idle,
    Starting,
    Running,
    Completed,
    Failed,
}

impl JobStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

struct State {
    auth_token: String,
    job_id: Option<String>,
    status: JobStatus,
    exit_code: Option<i32>,
    // PID of the running child. Set by /exec, cleared by the waiter when the
    // child is reaped. We keep only the pid (not the Child) here so /kill can
    // signal the process without racing the waiter for ownership of `Child`.
    child_pid: Option<i32>,
    last_heartbeat: Option<Instant>,
}

impl State {
    fn new(auth_token: String) -> Self {
        Self {
            auth_token,
            job_id: None,
            status: JobStatus::Idle,
            exit_code: None,
            child_pid: None,
            last_heartbeat: None,
        }
    }
}

type SharedState = Arc<Mutex<State>>;

// --- JSON: hand-rolled. Payloads are 2-3 string/int fields, no nesting. ---

fn json_escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn status_json(state: &State) -> String {
    let mut s = String::from("{\"id\":");
    match &state.job_id {
        Some(id) => json_escape(id, &mut s),
        None => s.push_str("null"),
    }
    s.push_str(",\"status\":");
    json_escape(state.status.as_str(), &mut s);
    s.push_str(",\"exit_code\":");
    match state.exit_code {
        Some(c) => s.push_str(&c.to_string()),
        None => s.push_str("null"),
    }
    s.push('}');
    s
}

// Minimal extractor for {"key1":"val1","key2":"val2"}. Handles whitespace
// and the escapes our generator might emit; rejects anything fancier with None.
fn parse_simple_obj(body: &str) -> Option<std::collections::HashMap<String, String>> {
    let body = body.trim();
    let body = body.strip_prefix('{')?.strip_suffix('}')?;
    let mut out = std::collections::HashMap::new();
    let mut chars = body.chars().peekable();
    loop {
        while let Some(c) = chars.peek() {
            if c.is_whitespace() || *c == ',' {
                chars.next();
            } else {
                break;
            }
        }
        if chars.peek().is_none() {
            return Some(out);
        }
        let key = parse_json_string(&mut chars)?;
        while let Some(c) = chars.peek() {
            if c.is_whitespace() {
                chars.next();
            } else {
                break;
            }
        }
        if chars.next()? != ':' {
            return None;
        }
        while let Some(c) = chars.peek() {
            if c.is_whitespace() {
                chars.next();
            } else {
                break;
            }
        }
        let value = parse_json_string(&mut chars)?;
        out.insert(key, value);
    }
}

fn parse_json_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    if chars.next()? != '"' {
        return None;
    }
    let mut out = String::new();
    loop {
        let c = chars.next()?;
        match c {
            '"' => return Some(out),
            '\\' => {
                let esc = chars.next()?;
                match esc {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    '/' => out.push('/'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'u' => {
                        let mut code = 0u32;
                        for _ in 0..4 {
                            let hex = chars.next()?;
                            code = code * 16 + hex.to_digit(16)?;
                        }
                        out.push(char::from_u32(code)?);
                    }
                    _ => return None,
                }
            }
            c => out.push(c),
        }
    }
}

// --- HTTP helpers ---

fn json_response(body: String, status: u16) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_string(body)
        .with_status_code(status)
        .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap())
}

fn empty(status: u16) -> Response<std::io::Empty> {
    Response::empty(status)
}

fn check_auth(req: &Request, expected: &str) -> bool {
    req.headers().iter().any(|h| {
        h.field
            .as_str()
            .as_str()
            .eq_ignore_ascii_case("X-Auth-Token")
            && h.value.as_str() == expected
    })
}

fn path_only(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

fn query_param<'a>(url: &'a str, key: &str) -> Option<&'a str> {
    let qs = url.split_once('?')?.1;
    for pair in qs.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(v);
            }
        }
    }
    None
}

// --- /proc/self/oom_score_adj helpers ---

fn write_oom_score_adj_self(value: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .write(true)
        .open("/proc/self/oom_score_adj")
    {
        let _ = f.write_all(value.as_bytes());
    }
}

/// Async-signal-safe reset of the child's oom_score_adj. Runs in the child
/// between fork() and execve() — only async-signal-safe calls allowed here
/// (open/write/close, no allocation, no logging).
unsafe fn reset_child_oom_score() -> std::io::Result<()> {
    let path = b"/proc/self/oom_score_adj\0";
    let fd = libc::open(path.as_ptr() as *const _, libc::O_WRONLY);
    if fd < 0 {
        return Ok(()); // best effort; OK if /proc isn't there in tests
    }
    let buf = b"0";
    libc::write(fd, buf.as_ptr() as *const _, buf.len());
    libc::close(fd);
    Ok(())
}

// --- Subprocess lifecycle ---

fn spawn_job(job_id: &str, script_path: &str) -> std::io::Result<Child> {
    fs::create_dir_all(LOG_DIR)?;
    let stdout_file = File::create(format!("{LOG_DIR}/{job_id}.stdout"))?;
    let stderr_file = File::create(format!("{LOG_DIR}/{job_id}.stderr"))?;

    let mut cmd = Command::new("sh");
    cmd.args(["-e", script_path])
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));

    unsafe {
        cmd.pre_exec(|| {
            // New session so killpg() can take down the whole group.
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Don't inherit our OOM immunity — user job must remain an OOM candidate.
            reset_child_oom_score()?;
            Ok(())
        });
    }
    cmd.spawn()
}

// Waiter owns the Child. It is the only place that calls Child::wait(), so
// only one thread ever reaps. /kill signals via raw libc and waits for the
// waiter to update the state.
fn wait_for_process(state: SharedState, job_id: String, mut child: Child) {
    let code = child.wait().ok().and_then(|st| st.code()).unwrap_or(-1);
    let mut s = state.lock().unwrap();
    if s.job_id.as_deref() == Some(&job_id) {
        s.exit_code = Some(code);
        s.status = if code == 0 {
            JobStatus::Completed
        } else {
            JobStatus::Failed
        };
        s.child_pid = None;
    }
}

fn kill_running(state: &SharedState) {
    // Snapshot pid + job_id so we can detect whether the waiter has progressed
    // for the same job by the time we re-check.
    let (pid, job_id) = {
        let s = state.lock().unwrap();
        match (s.child_pid, s.job_id.clone()) {
            (Some(pid), Some(jid)) if s.exit_code.is_none() => (pid, jid),
            _ => return,
        }
    };

    unsafe {
        let pgid = libc::getpgid(pid);
        if pgid < 0 {
            return;
        }
        libc::killpg(pgid, libc::SIGTERM);

        let deadline = Instant::now() + KILL_GRACE;
        loop {
            // Did the waiter notice the death and record the exit code?
            {
                let s = state.lock().unwrap();
                if s.job_id.as_deref() == Some(&job_id) && s.exit_code.is_some() {
                    return;
                }
            }
            if Instant::now() >= deadline {
                libc::killpg(pgid, libc::SIGKILL);
                // After SIGKILL the waiter will reap; give it a brief moment
                // so /kill's status snapshot reflects the death.
                let kill_deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < kill_deadline {
                    let s = state.lock().unwrap();
                    if s.job_id.as_deref() == Some(&job_id) && s.exit_code.is_some() {
                        return;
                    }
                    drop(s);
                    thread::sleep(Duration::from_millis(50));
                }
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

// --- Route handlers ---

fn handle_health(req: Request) {
    let _ = req.respond(json_response(r#"{"status":"ok"}"#.into(), 200));
}

fn handle_status(req: Request, state: &SharedState) {
    let s = state.lock().unwrap();
    let body = status_json(&s);
    drop(s);
    let _ = req.respond(json_response(body, 200));
}

fn handle_heartbeat(req: Request, state: &SharedState) {
    state.lock().unwrap().last_heartbeat = Some(Instant::now());
    let _ = req.respond(json_response(r#"{"status":"ok"}"#.into(), 200));
}

fn handle_exec(mut req: Request, state: &SharedState) {
    let mut body = String::new();
    if req.as_reader().read_to_string(&mut body).is_err() {
        let _ = req.respond(json_response(r#"{"error":"read failed"}"#.into(), 400));
        return;
    }
    let Some(obj) = parse_simple_obj(&body) else {
        let _ = req.respond(json_response(r#"{"error":"invalid body"}"#.into(), 400));
        return;
    };
    let (Some(job_id), Some(path)) = (obj.get("id"), obj.get("path")) else {
        let _ = req.respond(json_response(
            r#"{"error":"missing id or path"}"#.into(),
            400,
        ));
        return;
    };

    // Step 1: under lock, reserve the slot with a "starting" sentinel.
    let prev_status = {
        let mut s = state.lock().unwrap();
        if matches!(s.status, JobStatus::Running | JobStatus::Starting) {
            let _ = req.respond(json_response(
                r#"{"error":"A job is already running"}"#.into(),
                409,
            ));
            return;
        }
        let prev = s.status;
        s.status = JobStatus::Starting;
        prev
    };

    // Step 2: outside lock, do the I/O (open log files, fork+exec).
    let child = match spawn_job(job_id, path) {
        Ok(c) => c,
        Err(e) => {
            state.lock().unwrap().status = prev_status;
            let mut body = String::from(r#"{"error":"#);
            json_escape(&e.to_string(), &mut body);
            body.push('}');
            let _ = req.respond(json_response(body, 500));
            return;
        }
    };

    let pid = child.id() as i32;

    // Step 3: commit running state under lock.
    {
        let mut s = state.lock().unwrap();
        s.job_id = Some(job_id.clone());
        s.status = JobStatus::Running;
        s.exit_code = None;
        s.child_pid = Some(pid);
        s.last_heartbeat = Some(Instant::now());
    }

    let waiter_state = Arc::clone(state);
    let waiter_id = job_id.clone();
    thread::spawn(move || wait_for_process(waiter_state, waiter_id, child));

    let mut body = String::from(r#"{"id":"#);
    json_escape(job_id, &mut body);
    body.push_str(r#","status":"running"}"#);
    let _ = req.respond(json_response(body, 200));
}

fn handle_kill(req: Request, state: &SharedState) {
    kill_running(state);
    let body = status_json(&state.lock().unwrap());
    let _ = req.respond(json_response(body, 200));
}

fn handle_logs(req: Request, state: &SharedState) {
    let url = req.url().to_string();
    let offset: u64 = query_param(&url, "offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let stream = match query_param(&url, "stream") {
        Some("stderr") => "stderr",
        _ => "stdout",
    };

    let job_id = state.lock().unwrap().job_id.clone();
    let Some(job_id) = job_id else {
        respond_bytes(req, Vec::new(), false);
        return;
    };

    let path: PathBuf = format!("{LOG_DIR}/{job_id}.{stream}").into();
    let Ok(mut f) = File::open(&path) else {
        respond_bytes(req, Vec::new(), false);
        return;
    };
    let size = f.metadata().map(|m| m.len()).unwrap_or(0);
    if offset >= size {
        respond_bytes(req, Vec::new(), false);
        return;
    }
    if f.seek(SeekFrom::Start(offset)).is_err() {
        respond_bytes(req, Vec::new(), false);
        return;
    }
    let mut buf = vec![0u8; MAX_CHUNK];
    let n = f.read(&mut buf).unwrap_or(0);
    buf.truncate(n);
    let remaining = size.saturating_sub(offset + n as u64);
    respond_bytes(req, buf, remaining > 0);
}

fn respond_bytes(req: Request, data: Vec<u8>, more: bool) {
    let resp = Response::from_data(data).with_header(
        Header::from_bytes(&b"Content-Type"[..], &b"application/octet-stream"[..]).unwrap(),
    );
    let resp = if more {
        resp.with_header(Header::from_bytes(&b"X-More-Data"[..], &b"true"[..]).unwrap())
    } else {
        resp.with_header(Header::from_bytes(&b"X-More-Data"[..], &b"false"[..]).unwrap())
    };
    let _ = req.respond(resp);
}

// --- Dispatcher ---

fn dispatch(req: Request, state: SharedState) {
    let path = path_only(req.url()).to_string();
    let method = req.method().clone();

    if method == Method::Get && path == "/health" {
        return handle_health(req);
    }

    let token = { state.lock().unwrap().auth_token.clone() };
    // /exec, /kill, /heartbeat all require auth; /status and /logs too.
    if !check_auth(&req, &token) {
        let _ = req.respond(empty(403));
        return;
    }

    match (method, path.as_str()) {
        (Method::Get, "/status") => handle_status(req, &state),
        (Method::Get, "/logs") => handle_logs(req, &state),
        (Method::Post, "/exec") => handle_exec(req, &state),
        (Method::Post, "/kill") => handle_kill(req, &state),
        (Method::Post, "/heartbeat") => handle_heartbeat(req, &state),
        _ => {
            let _ = req.respond(empty(404));
        }
    }
}

// --- Watchdog + signals + main ---

fn start_heartbeat_watchdog(state: SharedState) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(5));
        let should_kill = {
            let s = state.lock().unwrap();
            matches!(s.last_heartbeat, Some(t) if t.elapsed() > HEARTBEAT_TIMEOUT)
        };
        if should_kill {
            eprintln!(
                "Heartbeat timeout: no heartbeat for {}s, killing current process",
                HEARTBEAT_TIMEOUT.as_secs()
            );
            kill_running(&state);
            state.lock().unwrap().last_heartbeat = None;
        }
    });
}

// Signal handler must be async-signal-safe, so it does the minimum: just
// _exit(0). It does NOT kill the running child/job — the child lives in its
// own session (see pre_exec setsid), so killpg wouldn't reach it anyway, and
// tracking the pid to kill it here would mean touching a mutex from signal
// context, which isn't async-signal-safe. On SIGTERM/SIGINT the pod is being
// torn down, so the kernel reaps this process and pod teardown GCs whatever
// job process remains. We forgo a clean listener shutdown for the same reason.
extern "C" fn signal_handler(_signum: libc::c_int) {
    unsafe {
        libc::_exit(0);
    }
}

fn install_signal_handlers() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = signal_handler as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
    }
}

fn parse_args() -> (u16, String) {
    let mut port: u16 = 8080;
    let mut token: Option<String> = None;
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--port" => {
                port = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("--port requires a number");
            }
            "--token" => token = args.next(),
            other => panic!("unknown arg: {other}"),
        }
    }
    (port, token.expect("--token is required"))
}

fn bind_dual_stack(port: u16) -> std::io::Result<TcpListener> {
    use libc::{c_int, c_void, socklen_t};
    unsafe {
        let fd = libc::socket(libc::AF_INET6, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SO_REUSEADDR for fast restarts in retry loops.
        let yes: c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &yes as *const _ as *const c_void,
            std::mem::size_of_val(&yes) as socklen_t,
        );
        // Dual-stack: accept both v4 and v6 connections.
        let off: c_int = 0;
        libc::setsockopt(
            fd,
            libc::IPPROTO_IPV6,
            libc::IPV6_V6ONLY,
            &off as *const _ as *const c_void,
            std::mem::size_of_val(&off) as socklen_t,
        );

        let mut addr: libc::sockaddr_in6 = std::mem::zeroed();
        // sin6_family is u16 on Linux, u8 on macOS (BSDs have sin6_len first).
        addr.sin6_family = libc::AF_INET6 as _;
        addr.sin6_port = port.to_be();
        if libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of_val(&addr) as socklen_t,
        ) < 0
        {
            let e = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }
        if libc::listen(fd, 128) < 0 {
            let e = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }
        Ok(TcpListener::from_raw_fd(fd))
    }
}

fn main() {
    let (port, token) = parse_args();

    write_oom_score_adj_self("-1000");
    let _ = fs::create_dir_all(LOG_DIR);
    install_signal_handlers();

    let state = Arc::new(Mutex::new(State::new(token)));
    start_heartbeat_watchdog(Arc::clone(&state));

    let listener = bind_dual_stack(port).expect("bind");
    let actual_port = listener.local_addr().expect("local_addr").port();
    if let Ok(mut f) = File::create(PORT_FILE) {
        let _ = f.write_all(actual_port.to_string().as_bytes());
    }
    println!("RPC server listening on [::]:{actual_port}");

    let server = Server::from_listener(listener, None).expect("server");
    let server = Arc::new(server);

    // Thread-per-request — matches the Python ThreadingHTTPServer model.
    for req in server.incoming_requests() {
        let s = Arc::clone(&state);
        thread::spawn(move || dispatch(req, s));
    }
}
