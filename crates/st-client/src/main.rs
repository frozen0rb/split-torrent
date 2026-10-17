#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod fmt;
mod settings;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title("Split Torrent")
            .with_inner_size([1060.0, 640.0])
            .with_min_inner_size([760.0, 420.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        settings::APP_ID,
        options,
        Box::new(|cc| Ok(Box::new(app::App::new(cc)))),
    )
}
