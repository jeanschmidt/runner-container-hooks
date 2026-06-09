// Integration tests for the rpc-server binary. Spawns a real instance on an
// OS-assigned port and drives the HTTP endpoints. Run via `cargo test --release`
// from packages/k8s/rpc-server/.

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const TOKEN: &str = "test-token";

struct Server {
    child: Child,
    port: u16,
    tmpdir: PathBuf,
}

impl Server {
    fn start() -> Self {
        let tmpdir = std::env::temp_dir().join(format!("rpc-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmpdir);

        let bin = env!("CARGO_BIN_EXE_rpc-server");
        let mut child = Command::new(bin)
            .args(["--port", "0", "--token", TOKEN])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rpc-server");

        let stdout = child.stdout.take().expect("stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        let port = loop {
            line.clear();
            if reader.read_line(&mut line).is_err() {
                panic!("rpc-server exited before printing port: {line}");
            }
            if let Some(rest) = line.strip_prefix("RPC server listening on [::]:") {
                break rest.trim().parse().expect("parse port");
            }
            if Instant::now() > deadline {
                panic!("rpc-server did not announce port in time");
            }
        };

        Self {
            child,
            port,
            tmpdir,
        }
    }

    fn raw(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&str>,
    ) -> (u16, String) {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).expect("connect");
        use std::io::Write;
        let body = body.unwrap_or("");
        let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");
        if let Some(t) = token {
            req.push_str(&format!("X-Auth-Token: {t}\r\n"));
        }
        if !body.is_empty() {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        req.push_str("Connection: close\r\n\r\n");
        req.push_str(body);
        stream.write_all(req.as_bytes()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut resp = String::new();
        use std::io::Read;
        let _ = stream.read_to_string(&mut resp);
        let status: u16 = resp
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = resp.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    fn write_script(&self, name: &str, body: &str) -> String {
        let p = self.tmpdir.join(name);
        std::fs::write(&p, body).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        p.to_string_lossy().into_owned()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmpdir);
        let _ = std::fs::remove_file("/tmp/rpc-server.port");
    }
}

#[test]
fn health_no_auth() {
    let s = Server::start();
    let (code, body) = s.raw("GET", "/health", None, None);
    assert_eq!(code, 200);
    assert!(body.contains(r#""status":"ok""#));
}

#[test]
fn status_requires_auth() {
    let s = Server::start();
    let (code, _) = s.raw("GET", "/status", None, None);
    assert_eq!(code, 403);
}

#[test]
fn status_idle_initially() {
    let s = Server::start();
    let (code, body) = s.raw("GET", "/status", Some(TOKEN), None);
    assert_eq!(code, 200);
    assert!(body.contains(r#""status":"idle""#));
    assert!(body.contains(r#""id":null"#));
    assert!(body.contains(r#""exit_code":null"#));
}

#[test]
fn exec_runs_to_completion() {
    let s = Server::start();
    let script = s.write_script("ok.sh", "#!/bin/sh\necho hi\nexit 0\n");
    let body = format!(r#"{{"id":"job1","path":"{script}"}}"#);
    let (code, body) = s.raw("POST", "/exec", Some(TOKEN), Some(&body));
    assert_eq!(code, 200);
    assert!(body.contains(r#""status":"running""#));

    // Wait for completion.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (_, st) = s.raw("GET", "/status", Some(TOKEN), None);
        if st.contains(r#""status":"completed""#) {
            assert!(st.contains(r#""exit_code":0"#));
            return;
        }
        if Instant::now() > deadline {
            panic!("never completed: {st}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn second_exec_409_while_running() {
    let s = Server::start();
    let long = s.write_script("long.sh", "#!/bin/sh\nsleep 30\n");
    let b1 = format!(r#"{{"id":"a","path":"{long}"}}"#);
    let (c1, _) = s.raw("POST", "/exec", Some(TOKEN), Some(&b1));
    assert_eq!(c1, 200);
    let b2 = format!(r#"{{"id":"b","path":"{long}"}}"#);
    let (c2, body2) = s.raw("POST", "/exec", Some(TOKEN), Some(&b2));
    assert_eq!(c2, 409);
    assert!(body2.contains("already running"));
}

#[test]
fn kill_terminates_running_job() {
    let s = Server::start();
    let long = s.write_script("kill.sh", "#!/bin/sh\nsleep 30\n");
    let body = format!(r#"{{"id":"k","path":"{long}"}}"#);
    let (c, _) = s.raw("POST", "/exec", Some(TOKEN), Some(&body));
    assert_eq!(c, 200);

    // Wait for state to reflect running before killing.
    thread::sleep(Duration::from_millis(100));

    let (kc, kb) = s.raw("POST", "/kill", Some(TOKEN), None);
    assert_eq!(kc, 200);
    assert!(
        kb.contains(r#""status":"failed""#),
        "kill response should reflect failed status, got: {kb}"
    );

    // A new exec should be accepted now.
    let ok = s.write_script("ok.sh", "#!/bin/sh\nexit 0\n");
    let body = format!(r#"{{"id":"after","path":"{ok}"}}"#);
    let (c, _) = s.raw("POST", "/exec", Some(TOKEN), Some(&body));
    assert_eq!(c, 200);
}

#[test]
fn logs_serve_stdout_and_stderr() {
    let s = Server::start();
    let script = s.write_script(
        "logs.sh",
        "#!/bin/sh\necho out1\necho err1 >&2\necho out2\n",
    );
    let body = format!(r#"{{"id":"logjob","path":"{script}"}}"#);
    s.raw("POST", "/exec", Some(TOKEN), Some(&body));

    // Wait for completion.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (_, st) = s.raw("GET", "/status", Some(TOKEN), None);
        if st.contains("completed") || st.contains("failed") {
            break;
        }
        if Instant::now() > deadline {
            panic!("never finished");
        }
        thread::sleep(Duration::from_millis(50));
    }

    let (_, out) = s.raw("GET", "/logs?stream=stdout", Some(TOKEN), None);
    assert!(
        out.contains("out1") && out.contains("out2"),
        "stdout: {out}"
    );
    let (_, err) = s.raw("GET", "/logs?stream=stderr", Some(TOKEN), None);
    assert!(err.contains("err1"), "stderr: {err}");
}

#[test]
fn heartbeat_accepted() {
    let s = Server::start();
    let (c, body) = s.raw("POST", "/heartbeat", Some(TOKEN), None);
    assert_eq!(c, 200);
    assert!(body.contains(r#""status":"ok""#));
}
