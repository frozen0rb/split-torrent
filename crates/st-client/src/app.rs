use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eframe::egui::{self, Color32, RichText};
use egui_extras::{Column, TableBuilder};
use st_client_core::{
    Client, ClientConfig, ConnectionState, Event, InfoHash, TorrentSnapshot, TorrentState,
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::fmt;
use crate::settings::{self, SavedTorrent, Settings};

const MAX_MESSAGES: usize = 100;

pub struct App {
    settings: Settings,
    rt: tokio::runtime::Runtime,
    client: Option<Client>,
    events: Option<UnboundedReceiver<Event>>,
    conn: Option<(ConnectionState, Instant)>,
    snapshots: HashMap<InfoHash, TorrentSnapshot>,
    buffer: Option<(u64, u64)>,
    torrent_path: String,
    show_token: bool,
    messages: VecDeque<(String, bool)>,
    dirty: bool,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let settings = cc
            .storage
            .and_then(|s| eframe::get_value(s, settings::SETTINGS_KEY))
            .unwrap_or_default();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        let mut app = Self {
            settings,
            rt,
            client: None,
            events: None,
            conn: None,
            snapshots: HashMap::new(),
            buffer: None,
            torrent_path: String::new(),
            show_token: false,
            messages: VecDeque::new(),
            dirty: false,
        };
        if app.settings.auto_connect {
            app.connect(&cc.egui_ctx);
        }
        app
    }

    fn message(&mut self, text: impl Into<String>, error: bool) {
        self.messages.push_back((text.into(), error));
        while self.messages.len() > MAX_MESSAGES {
            self.messages.pop_front();
        }
    }

    fn connect(&mut self, ctx: &egui::Context) {
        let s = &self.settings;
        let config = match ClientConfig::new(&s.server, &s.token, &s.fingerprint) {
            Ok(c) => c,
            Err(e) => {
                self.message(format!("Cannot connect: {e}"), true);
                return;
            }
        };
        let ctx = ctx.clone();
        let (client, events) = Client::start(
            config,
            self.rt.handle(),
            Some(Arc::new(move || ctx.request_repaint())),
        );
        for saved in self.settings.torrents.clone() {
            let res = std::fs::read(&saved.torrent_file)
                .map_err(|e| format!("{}: {e}", saved.torrent_file.display()))
                .and_then(|bytes| {
                    client
                        .add_torrent(bytes, &saved.download_dir, saved.paused)
                        .map_err(|e| e.to_string())
                });
            match res {
                Ok(snap) => {
                    self.snapshots.insert(snap.info_hash, snap);
                }
                Err(e) => self.message(format!("{}: {e}", saved.name), true),
            }
        }
        self.client = Some(client);
        self.events = Some(events);
        self.settings.auto_connect = true;
        self.dirty = true;
    }

    fn disconnect(&mut self) {
        if let Some(c) = self.client.take() {
            c.shutdown();
        }
        self.events = None;
        self.conn = None;
        self.buffer = None;
        for s in self.snapshots.values_mut() {
            s.online = false;
            s.server = None;
            s.recv_speed = 0;
        }
        self.settings.auto_connect = false;
        self.dirty = true;
    }

    fn pump_events(&mut self) {
        let Some(rx) = self.events.as_mut() else {
            return;
        };
        let mut evs = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            evs.push(ev);
        }
        for ev in evs {
            match ev {
                Event::Connection(state) => {
                    match &state {
                        ConnectionState::Connected { buffer_limit } => {
                            self.message("Connected to server", false);
                            self.buffer = Some((self.buffer.map_or(0, |b| b.0), *buffer_limit));
                        }
                        ConnectionState::Disconnected { error, .. } => {
                            self.message(format!("Disconnected: {error}"), true);
                        }
                        _ => {}
                    }
                    self.conn = Some((state, Instant::now()));
                }
                Event::Torrent(snap) => {
                    if let Some(s) = &snap.server {
                        self.buffer = Some((s.buffer_used, s.buffer_limit));
                    }
                    if let Some(i) = self.settings.find(&snap.info_hash) {
                        if self.settings.torrents[i].paused != snap.paused {
                            self.settings.torrents[i].paused = snap.paused;
                            self.dirty = true;
                        }
                    }
                    self.snapshots.insert(snap.info_hash, snap);
                }
                Event::Removed(ih) => {
                    self.snapshots.remove(&ih);
                }
                Event::Warning(w) => self.message(w, true),
            }
        }
    }

    fn add_torrent(&mut self) {
        let Some(client) = &self.client else {
            return;
        };
        let path = PathBuf::from(self.torrent_path.trim());
        let dir = PathBuf::from(self.settings.download_dir.trim());
        let res = std::fs::read(&path)
            .map_err(|e| format!("{}: {e}", path.display()))
            .and_then(|bytes| {
                let snap = client
                    .add_torrent(bytes.clone(), &dir, false)
                    .map_err(|e| e.to_string())?;
                let copy = settings::store_torrent_copy(
                    &settings::torrents_dir(),
                    &snap.info_hash,
                    &bytes,
                )
                .map_err(|e| format!("saving torrent copy: {e}"))?;
                Ok((snap, copy))
            });
        match res {
            Ok((snap, copy)) => {
                self.message(format!("Added {}", snap.name), false);
                self.settings.torrents.push(SavedTorrent {
                    info_hash: settings::hex(&snap.info_hash),
                    name: snap.name.clone(),
                    torrent_file: copy,
                    download_dir: dir,
                    paused: false,
                });
                self.snapshots.insert(snap.info_hash, snap);
                self.torrent_path.clear();
                self.dirty = true;
            }
            Err(e) => self.message(format!("Cannot add torrent: {e}"), true),
        }
    }

    fn remove(&mut self, ih: InfoHash, delete_files: bool) {
        if let Some(c) = &self.client {
            c.remove(ih, delete_files);
        }
        if let Some(i) = self.settings.find(&ih) {
            let t = self.settings.torrents.remove(i);
            let _ = std::fs::remove_file(&t.torrent_file);
            self.dirty = true;
        }
        self.snapshots.remove(&ih);
    }

    fn set_paused(&mut self, ih: InfoHash, paused: bool) {
        if let Some(c) = &self.client {
            if paused {
                c.pause(ih)
            } else {
                c.resume(ih)
            }
        }
        if let Some(i) = self.settings.find(&ih) {
            self.settings.torrents[i].paused = paused;
            self.dirty = true;
        }
    }

    fn connection_ui(&mut self, ui: &mut egui::Ui) {
        let connected = self.client.is_some();
        egui::Grid::new("connection")
            .num_columns(2)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                ui.label("Server address");
                ui.add_enabled(
                    !connected,
                    egui::TextEdit::singleline(&mut self.settings.server)
                        .hint_text("203.0.113.7:7443")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();
                ui.label("Token");
                ui.horizontal(|ui| {
                    ui.add_enabled(
                        !connected,
                        egui::TextEdit::singleline(&mut self.settings.token)
                            .password(!self.show_token)
                            .desired_width(ui.available_width() - 60.0),
                    );
                    ui.checkbox(&mut self.show_token, "Show");
                });
                ui.end_row();
                ui.label("Certificate SHA-256");
                ui.add_enabled(
                    !connected,
                    egui::TextEdit::singleline(&mut self.settings.fingerprint)
                        .hint_text("AB:CD:… as printed by st-server")
                        .font(egui::TextStyle::Monospace)
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();
            });
        ui.horizontal(|ui| {
            if connected {
                if ui.button("Disconnect").clicked() {
                    self.disconnect();
                }
            } else if ui.button("Connect").clicked() {
                self.connect(ui.ctx());
            }
            let (text, color) = match &self.conn {
                None => ("Not connected".to_string(), Color32::GRAY),
                Some((ConnectionState::Connecting, _)) => ("Connecting…".into(), Color32::GOLD),
                Some((ConnectionState::Connected { .. }, _)) => {
                    ("Connected".into(), Color32::from_rgb(80, 200, 120))
                }
                Some((ConnectionState::Disconnected { error, retry_in }, since)) => {
                    let left = retry_in.saturating_sub(since.elapsed()).as_secs();
                    (
                        format!("Disconnected: {error} (retry in {left}s)"),
                        Color32::from_rgb(230, 90, 80),
                    )
                }
                Some((ConnectionState::Stopped, _)) => ("Stopped".into(), Color32::GRAY),
            };
            let (rect, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
            ui.painter().circle_filled(rect.center(), 5.0, color);
            ui.add(egui::Label::new(RichText::new(text).color(color)).truncate());
        });
    }

    fn add_ui(&mut self, ui: &mut egui::Ui) {
        egui::Grid::new("add")
            .num_columns(2)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                ui.label("Torrent file");
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.torrent_path)
                            .hint_text("Pick or drop a .torrent file")
                            .desired_width(ui.available_width() - 90.0),
                    );
                    if ui.button("Browse…").clicked() {
                        if let Some(p) = rfd::FileDialog::new()
                            .add_filter("Torrent", &["torrent"])
                            .pick_file()
                        {
                            self.torrent_path = p.display().to_string();
                        }
                    }
                });
                ui.end_row();
                ui.label("Download folder");
                ui.horizontal(|ui| {
                    if ui
                        .add(
                            egui::TextEdit::singleline(&mut self.settings.download_dir)
                                .desired_width(ui.available_width() - 90.0),
                        )
                        .changed()
                    {
                        self.dirty = true;
                    }
                    if ui.button("Browse…").clicked() {
                        if let Some(p) = rfd::FileDialog::new()
                            .set_directory(&self.settings.download_dir)
                            .pick_folder()
                        {
                            self.settings.download_dir = p.display().to_string();
                            self.dirty = true;
                        }
                    }
                });
                ui.end_row();
            });
        let can_add = self.client.is_some()
            && !self.torrent_path.trim().is_empty()
            && !self.settings.download_dir.trim().is_empty();
        let hint = if self.client.is_none() {
            "Connect first"
        } else {
            "Choose a .torrent file and a download folder"
        };
        if ui
            .add_enabled(can_add, egui::Button::new("Add torrent"))
            .on_disabled_hover_text(hint)
            .clicked()
        {
            self.add_torrent();
        }
    }

    fn status_text(&self, s: &TorrentSnapshot) -> (String, Color32) {
        let connected = matches!(self.conn, Some((ConnectionState::Connected { .. }, _)));
        if let Some(e) = &s.error {
            return (format!("Error: {e}"), Color32::from_rgb(230, 90, 80));
        }
        if s.complete() {
            return ("Complete".into(), Color32::from_rgb(80, 200, 120));
        }
        if s.paused {
            return ("Paused".into(), Color32::GRAY);
        }
        if !connected {
            return ("Waiting for server".into(), Color32::GRAY);
        }
        match s.server.as_ref().map(|s| s.state) {
            None if !s.online => ("Registering…".into(), Color32::GOLD),
            None | Some(TorrentState::Initializing) => ("Initializing".into(), Color32::GOLD),
            Some(TorrentState::Downloading) => ("Downloading".into(), Color32::LIGHT_BLUE),
            Some(TorrentState::Paused) => ("Paused on server".into(), Color32::GRAY),
            Some(TorrentState::Finished) => ("Finishing".into(), Color32::LIGHT_BLUE),
            Some(TorrentState::Error) => ("Server error".into(), Color32::from_rgb(230, 90, 80)),
        }
    }

    fn torrents_ui(&mut self, ui: &mut egui::Ui) {
        let rows: Vec<(InfoHash, String, Option<TorrentSnapshot>, bool)> = self
            .settings
            .torrents
            .iter()
            .filter_map(|t| {
                let ih = parse_hex(&t.info_hash)?;
                Some((
                    ih,
                    t.name.clone(),
                    self.snapshots.get(&ih).cloned(),
                    t.paused,
                ))
            })
            .collect();
        if rows.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(
                    RichText::new("No torrents yet. Connect, then add a .torrent file.").weak(),
                );
            });
            return;
        }
        let mut action: Option<(InfoHash, Action)> = None;
        TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
            .column(Column::initial(230.0).at_least(120.0).clip(true))
            .column(Column::initial(80.0))
            .column(Column::initial(150.0))
            .column(Column::initial(130.0).clip(true))
            .column(Column::initial(85.0))
            .column(Column::initial(85.0))
            .column(Column::initial(50.0))
            .column(Column::remainder().at_least(150.0))
            .header(22.0, |mut h| {
                for title in [
                    "Name",
                    "Size",
                    "Progress",
                    "Status",
                    "Swarm speed",
                    "To this PC",
                    "Peers",
                    "",
                ] {
                    h.col(|ui| {
                        ui.strong(title);
                    });
                }
            })
            .body(|mut body| {
                for (ih, name, snap, paused) in rows {
                    body.row(26.0, |mut row| {
                        row.col(|ui| {
                            ui.label(&name).on_hover_text(&name);
                        });
                        let Some(s) = snap else {
                            row.col(|ui| {
                                ui.label("–");
                            });
                            row.col(|_| {});
                            row.col(|ui| {
                                ui.label(RichText::new("Not connected").weak());
                            });
                            for _ in 0..3 {
                                row.col(|_| {});
                            }
                            row.col(|ui| {
                                if ui.button("Remove").clicked() {
                                    action = Some((ih, Action::Remove(false)));
                                }
                            });
                            return;
                        };
                        row.col(|ui| {
                            ui.label(fmt::bytes(s.total_length));
                        });
                        row.col(|ui| {
                            ui.add(
                                egui::ProgressBar::new(s.progress())
                                    .text(format!("{:.1}%", s.progress() * 100.0)),
                            )
                            .on_hover_text(format!(
                                "{} of {} pieces, {} of {}",
                                s.pieces_have,
                                s.piece_count,
                                fmt::bytes(s.bytes_have),
                                fmt::bytes(s.total_length)
                            ));
                        });
                        let (text, color) = self.status_text(&s);
                        row.col(|ui| {
                            ui.label(RichText::new(&text).color(color))
                                .on_hover_text(&text);
                        });
                        let srv = s.server.as_ref();
                        row.col(|ui| {
                            ui.label(fmt::speed(srv.map_or(0, |v| v.down_speed)));
                        });
                        row.col(|ui| {
                            ui.label(fmt::speed(s.recv_speed));
                        });
                        row.col(|ui| {
                            ui.label(srv.map_or("–".into(), |v| v.peers.to_string()));
                        });
                        row.col(|ui| {
                            if !s.complete() {
                                let label = if paused { "Resume" } else { "Pause" };
                                if ui.button(label).clicked() {
                                    action = Some((ih, Action::Pause(!paused)));
                                }
                            }
                            ui.menu_button("Remove", |ui| {
                                if ui.button("Keep downloaded files").clicked() {
                                    action = Some((ih, Action::Remove(false)));
                                }
                                if ui.button("Delete downloaded files").clicked() {
                                    action = Some((ih, Action::Remove(true)));
                                }
                            });
                            if ui
                                .button("Open")
                                .on_hover_text("Open download folder")
                                .clicked()
                            {
                                action = Some((ih, Action::Open(s.download_dir.clone())));
                            }
                        });
                    });
                }
            });
        match action {
            Some((ih, Action::Pause(p))) => self.set_paused(ih, p),
            Some((ih, Action::Remove(delete))) => self.remove(ih, delete),
            Some((_, Action::Open(dir))) => open_folder(&dir),
            None => {}
        }
    }

    fn footer_ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Server RAM buffer");
            match self.buffer {
                Some((used, limit)) if limit > 0 => {
                    ui.add(
                        egui::ProgressBar::new(used as f32 / limit as f32)
                            .desired_width(220.0)
                            .text(format!("{} / {}", fmt::bytes(used), fmt::bytes(limit))),
                    );
                }
                _ => {
                    ui.label(RichText::new("–").weak());
                }
            }
            let total_rx: u64 = self.snapshots.values().map(|s| s.recv_speed).sum();
            ui.separator();
            ui.label(format!("Receiving {}", fmt::speed(total_rx)));
        });
        egui::CollapsingHeader::new(format!("Messages ({})", self.messages.len()))
            .id_salt("messages")
            .show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .max_height(110.0)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for (m, err) in &self.messages {
                            let t = RichText::new(m).small();
                            ui.label(if *err {
                                t.color(Color32::from_rgb(230, 90, 80))
                            } else {
                                t
                            });
                        }
                    });
            });
    }
}

enum Action {
    Pause(bool),
    Remove(bool),
    Open(PathBuf),
}

fn parse_hex(s: &str) -> Option<InfoHash> {
    if s.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn open_folder(dir: &std::path::Path) {
    let program = if cfg!(windows) {
        "explorer"
    } else if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    let _ = std::process::Command::new(program).arg(dir).spawn();
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame) {
        self.pump_events();
        let dropped: Vec<PathBuf> = ui.input(|i| {
            i.raw
                .dropped_files
                .iter()
                .map(|f| f.path().to_path_buf())
                .collect()
        });
        if let Some(p) = dropped.into_iter().next() {
            self.torrent_path = p.display().to_string();
        }

        egui::Panel::top("connection")
            .frame(egui::Frame::side_top_panel(ui.style()).inner_margin(10.0))
            .show(ui, |ui| self.connection_ui(ui));
        egui::Panel::top("add")
            .frame(egui::Frame::side_top_panel(ui.style()).inner_margin(10.0))
            .show(ui, |ui| self.add_ui(ui));
        egui::Panel::bottom("footer")
            .frame(egui::Frame::side_top_panel(ui.style()).inner_margin(8.0))
            .show(ui, |ui| self.footer_ui(ui));
        egui::CentralPanel::default_margins().show(ui, |ui| self.torrents_ui(ui));

        if self.dirty {
            if let Some(storage) = frame.storage_mut() {
                eframe::set_value(storage, settings::SETTINGS_KEY, &self.settings);
                storage.flush();
            }
            self.dirty = false;
        }
        if matches!(self.conn, Some((ConnectionState::Disconnected { .. }, _))) {
            ui.ctx().request_repaint_after(Duration::from_millis(500));
        }
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, settings::SETTINGS_KEY, &self.settings);
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        let Some(c) = self.client.take() else {
            return;
        };
        c.shutdown();
        // Let the client flush pending writes and persist resume state.
        let deadline = Instant::now() + Duration::from_secs(3);
        while let Some(rx) = self.events.as_mut() {
            match rx.try_recv() {
                Ok(Event::Connection(ConnectionState::Stopped))
                | Err(TryRecvError::Disconnected) => break,
                Ok(_) => {}
                Err(TryRecvError::Empty) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(TryRecvError::Empty) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn hex_roundtrip() {
        let ih = [
            0x01, 0xfe, 0x7a, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff,
        ];
        assert_eq!(super::parse_hex(&crate::settings::hex(&ih)), Some(ih));
        assert_eq!(super::parse_hex("zz"), None);
        assert_eq!(super::parse_hex(&"g".repeat(40)), None);
    }
}
