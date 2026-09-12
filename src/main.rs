use macroquad::prelude::*;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use std::fs::File;

const DEFAULT_ADDR: &str = "192.168.0.10:10940";
const MD_COMMAND: &str = "MD0000108001000\n";

const START_STEP: i32 = 0;
const END_STEP: i32 = 1080;
const FRONT_STEP: i32 = 540;
const STEPS_PER_REV: f32 = 1440.0;
const MAX_RANGE_MM: u32 = 2_000;

#[derive(Clone)]
struct Scan {
    distances_mm: Vec<u32>,
    sensor_timestamp: u32,
    received_at: Instant,
}

struct SharedState {
    latest: Option<Scan>,
    status: String,
    frames: u64,
    parse_errors: u64,
    reconnects: u64,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            latest: None,
            status: "starting".to_string(),
            frames: 0,
            parse_errors: 0,
            reconnects: 0,
        }
    }
}

struct ScipReader {
    wr_file: File,
    file_rx: File,
    stream: TcpStream,
    buf: Vec<u8>,
}

impl ScipReader {
    fn new(stream: TcpStream) -> Self {
        let mut wr_file = File::create("wr_stream.bin").unwrap();
        let mut file_rx = File::create("stream_rx.bin").unwrap();
        Self {
            wr_file,
            file_rx,
            stream,
            buf: Vec::with_capacity(8192),
        }
    }

    fn send(&mut self, text: &str) -> io::Result<()> {
        self.wr_file.write(text.as_bytes())?;
        let _ = self.wr_file.flush();
        self.stream.write_all(text.as_bytes())?;
        self.stream.flush()
    }

    /// SCIP replies/frames are terminated by an empty line: \n\n.
    fn read_block(&mut self) -> io::Result<Vec<String>> {
        loop {
            if let Some(pos) = find_double_lf(&self.buf) {
                let raw: Vec<u8> = self.buf.drain(..pos + 2).collect();
                let text = String::from_utf8_lossy(&raw);
                let lines = text
                    .split('\n')
                    .map(|s| s.trim_end_matches('\r').to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                return Ok(lines);
            }

            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp)?;
            self.file_rx.write(&tmp)?;
            let _ = self.file_rx.flush();
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "LiDAR closed TCP connection",
                ));
            }
            self.buf.extend_from_slice(&tmp[..n]);

            // A corrupt stream should never grow anywhere near this size.
            if self.buf.len() > 128 * 1024 {
                self.buf.clear();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SCIP receive buffer overflow / lost framing",
                ));
            }
        }
    }
}

fn find_double_lf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

fn scip_decode(bytes: &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for &b in bytes {
        if !(0x30..=0x6f).contains(&b) {
            return None;
        }
        value = (value << 6) | u32::from(b - 0x30);
    }
    Some(value)
}

fn checksum_ok(line: &[u8]) -> bool {
    if line.len() < 2 {
        return false;
    }
    let data = &line[..line.len() - 1];
    let got = line[line.len() - 1];
    let sum = data.iter().fold(0u32, |acc, &b| acc + u32::from(b));
    let expected = ((sum & 0x3f) as u8) + 0x30;
    got == expected
}

fn parse_md_frame(lines: &[String]) -> Result<Option<Scan>, String> {
    if lines.len() < 2 {
        return Ok(None);
    }

    // The first block after MD is just the command acknowledgement:
    // MD.... / 00P
    if lines[1].starts_with("00") {
        return Ok(None);
    }

    // Continuous data frame:
    // MD....
    // 99<checksum>
    // <4-byte timestamp><checksum>
    // <up to 64 data chars><checksum>
    // ...
    if !lines[0].starts_with("MD") {
        return Ok(None);
    }
    if !lines[1].starts_with("99") {
        return Err(format!("SCIP MD status: {}", lines[1]));
    }
    if !checksum_ok(lines[1].as_bytes()) {
        return Err("bad SCIP status checksum".into());
    }
    if lines.len() < 4 {
        return Err("short SCIP MD frame".into());
    }

    let ts_line = lines[2].as_bytes();
    if ts_line.len() != 5 || !checksum_ok(ts_line) {
        return Err("bad SCIP timestamp line".into());
    }
    let sensor_timestamp = scip_decode(&ts_line[..4]).ok_or("bad SCIP timestamp encoding")?;

    let mut encoded = Vec::<u8>::with_capacity(3300);
    for line in &lines[3..] {
        let b = line.as_bytes();
        if b.len() < 2 || !checksum_ok(b) {
            return Err("bad SCIP data checksum".into());
        }
        // Last byte of every physical line is its checksum. Data groups may be
        // split across line boundaries, so concatenate first and decode later.
        encoded.extend_from_slice(&b[..b.len() - 1]);
    }

    let expected_points = (END_STEP - START_STEP + 1) as usize;
    let expected_bytes = expected_points * 3;
    if encoded.len() != expected_bytes {
        return Err(format!(
            "unexpected scan size: {} encoded bytes, expected {}",
            encoded.len(), expected_bytes
        ));
    }

    let mut distances_mm = Vec::with_capacity(expected_points);
    for chunk in encoded.chunks_exact(3) {
        distances_mm.push(scip_decode(chunk).ok_or("bad SCIP distance encoding")?);
    }

    Ok(Some(Scan {
        distances_mm,
        sensor_timestamp,
        received_at: Instant::now(),
    }))
}

fn set_status(shared: &Arc<Mutex<SharedState>>, status: impl Into<String>) {
    if let Ok(mut s) = shared.lock() {
        s.status = status.into();
    }
}

fn run_session(addr: &str, shared: &Arc<Mutex<SharedState>>) -> Result<(), String> {
    set_status(shared, format!("connecting to {addr}"));

    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|e| format!("bad address {addr}: {e}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve {addr}"))?;

    let stream = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(2))
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| format!("set_write_timeout: {e}"))?;
    let _ = stream.set_nodelay(true);

    let mut scip = ScipReader::new(stream);

    // MD: start=0000, end=1080, grouping=01, scan skip=0,
    // number of scans=00 => stream indefinitely until QT/disconnect.
    scip.send(MD_COMMAND)
        .map_err(|e| format!("send MD: {e}"))?;

    set_status(shared, format!("connected {addr}; waiting for scans"));

    loop {
        let block = scip.read_block().map_err(|e| format!("read: {e}"))?;
        match parse_md_frame(&block) {
            Ok(Some(scan)) => {
                if let Ok(mut s) = shared.lock() {
                    s.frames += 1;
                    s.latest = Some(scan);
                    s.status = format!("connected {addr} — streaming MD @ 40 Hz");
                }
            }
            Ok(None) => {}
            Err(e) => {
                if let Ok(mut s) = shared.lock() {
                    s.parse_errors += 1;
                    s.status = format!("connected, dropped malformed frame: {e}");
                }
                // A single malformed frame is not worth killing the TCP session.
            }
        }
    }
}

fn lidar_worker(addr: String, shared: Arc<Mutex<SharedState>>) {
    loop {
        if let Err(e) = run_session(&addr, &shared) {
            if let Ok(mut s) = shared.lock() {
                s.reconnects += 1;
                s.status = format!("{e}; reconnecting in 1 s");
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}

fn window_conf() -> Conf {
    Conf {
        window_title: "Hokuyo UST-30LX — SCIP polar viewer".to_owned(),
        window_width: 900,
        window_height: 900,
        window_resizable: true,
        ..Default::default()
    }
}

fn polar_to_screen(cx: f32, cy: f32, radius_px: f32, distance_mm: u32, angle_deg: f32, mirror: bool) -> (f32, f32) {
    let r = (distance_mm as f32 / MAX_RANGE_MM as f32) * radius_px;
    let a = angle_deg.to_radians();

    // 0° is straight ahead (up on screen). Positive SCIP angle is drawn to
    // the left by default; M toggles mirroring if the physical mounting/view
    // convention is opposite.
    let side = if mirror { 1.0 } else { -1.0 };
    let x = cx + side * r * a.sin();
    let y = cy - r * a.cos();
    (x, y)
}

fn draw_polar_grid(cx: f32, cy: f32, radius: f32, mirror: bool) {
    for ring in 1..=4 {
        let r = radius * ring as f32 / 4.0;
        draw_circle_lines(cx, cy, r, 1.0, Color::new(0.25, 0.28, 0.32, 1.0));
        let label = format!("{:.1} m", ring as f32 * 0.5);
        draw_text(
            &label,
            cx + 5.0,
            cy - r + 16.0,
            18.0,
            Color::new(0.65, 0.68, 0.72, 1.0),
        );
    }

    for angle in [-135.0f32, -90.0, -45.0, 0.0, 45.0, 90.0, 135.0] {
        let (x, y) = polar_to_screen(cx, cy, radius, MAX_RANGE_MM, angle, mirror);
        draw_line(cx, cy, x, y, 1.0, Color::new(0.25, 0.28, 0.32, 1.0));

        let (lx, ly) = polar_to_screen(cx, cy, radius + 1.0, MAX_RANGE_MM, angle, mirror);
        let text = format!("{angle:.0}°");
        draw_text(
            &text,
            lx - 15.0,
            ly + 5.0,
            16.0,
            Color::new(0.55, 0.58, 0.62, 1.0),
        );
    }

    draw_circle(cx, cy, 4.0, Color::new(1.0, 0.8, 0.2, 1.0));
    draw_line(cx, cy, cx, cy - 18.0, 2.0, Color::new(1.0, 0.8, 0.2, 1.0));
}

#[macroquad::main(window_conf)]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_string());

    let shared = Arc::new(Mutex::new(SharedState::default()));
    {
        let shared = Arc::clone(&shared);
        let addr = addr.clone();
        thread::spawn(move || lidar_worker(addr, shared));
    }

    let mut mirror = false;

    loop {
        if is_key_pressed(KeyCode::Escape) {
            break;
        }
        if is_key_pressed(KeyCode::M) {
            mirror = !mirror;
        }

        clear_background(Color::new(0.035, 0.045, 0.055, 1.0));

        let w = screen_width();
        let h = screen_height();
        let margin = 70.0;
        let radius = ((w.min(h) * 0.5) - margin).max(80.0);
        let cx = w * 0.5;
        let cy = h * 0.52;

        draw_polar_grid(cx, cy, radius, mirror);

        let (scan, status, frames, parse_errors, reconnects) = {
            let s = shared.lock().unwrap();
            (
                s.latest.clone(),
                s.status.clone(),
                s.frames,
                s.parse_errors,
                s.reconnects,
            )
        };

        let mut visible_points = 0usize;
        let mut age_ms = None;
        let mut timestamp = None;

        if let Some(scan) = scan {
            age_ms = Some(scan.received_at.elapsed().as_millis());
            timestamp = Some(scan.sensor_timestamp);

            for (i, &distance_mm) in scan.distances_mm.iter().enumerate() {
                if distance_mm < 10 || distance_mm > MAX_RANGE_MM {
                    continue;
                }
                let step = START_STEP + i as i32;
                let angle_deg = (step - FRONT_STEP) as f32 * 360.0 / STEPS_PER_REV;
                let (x, y) = polar_to_screen(cx, cy, radius, distance_mm, angle_deg, mirror);
                draw_circle(x, y, 1.8, Color::new(0.2, 1.0, 0.55, 1.0));
                visible_points += 1;
            }
        }

        draw_text(
            "UST-30LX  |  range 0..2.0 m  |  270° / 0.25°  |  M: mirror  Esc: quit",
            18.0,
            28.0,
            22.0,
            Color::new(0.85, 0.88, 0.92, 1.0),
        );
        draw_text(
            &status,
            18.0,
            54.0,
            19.0,
            Color::new(0.70, 0.78, 0.88, 1.0),
        );

        let telemetry = format!(
            "frames: {frames}   visible: {visible_points}   scan age: {}   sensor ts: {}   parse errors: {parse_errors}   reconnects: {reconnects}   UI: {} FPS",
            age_ms.map(|x| format!("{x} ms")).unwrap_or_else(|| "—".into()),
            timestamp.map(|x| x.to_string()).unwrap_or_else(|| "—".into()),
            get_fps()
        );
        draw_text(
            &telemetry,
            18.0,
            h - 18.0,
            17.0,
            Color::new(0.62, 0.65, 0.70, 1.0),
        );

        next_frame().await;
    }
}
