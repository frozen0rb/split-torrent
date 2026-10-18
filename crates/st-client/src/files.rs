//! File-selection dialog: the torrent's files as a tree with checkboxes.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use eframe::egui::{self, RichText};
use st_client_core::{InfoHash, Metainfo};

use crate::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Dir {
        /// Path from the torrent root, unique among directories.
        id: String,
        name: String,
        size: u64,
        /// Every file below this directory.
        files: Vec<usize>,
        children: Vec<Node>,
    },
    File {
        name: String,
        index: usize,
        size: u64,
    },
}

#[derive(Default)]
struct DirBuilder {
    dirs: BTreeMap<String, DirBuilder>,
    files: Vec<(String, usize, u64)>,
}

impl DirBuilder {
    fn build(self, prefix: &str) -> Vec<Node> {
        let mut out = Vec::new();
        let mut dirs: Vec<_> = self.dirs.into_iter().collect();
        dirs.sort_by_key(|(name, _)| name.to_lowercase());
        for (name, d) in dirs {
            let id = format!("{prefix}{name}/");
            let children = d.build(&id);
            let mut files = Vec::new();
            let mut size = 0;
            for c in &children {
                match c {
                    Node::Dir {
                        files: f, size: s, ..
                    } => {
                        files.extend_from_slice(f);
                        size += s;
                    }
                    Node::File { index, size: s, .. } => {
                        files.push(*index);
                        size += s;
                    }
                }
            }
            out.push(Node::Dir {
                id,
                name,
                size,
                files,
                children,
            });
        }
        let mut files = self.files;
        files.sort_by_key(|(name, ..)| name.to_lowercase());
        out.extend(
            files
                .into_iter()
                .map(|(name, index, size)| Node::File { name, index, size }),
        );
        out
    }
}

/// Builds the tree below the torrent root. Padding files are left out.
pub fn build_tree(meta: &Metainfo) -> Vec<Node> {
    let mut root = DirBuilder::default();
    for (i, f) in meta.files.iter().enumerate().filter(|(_, f)| !f.pad) {
        let comps: Vec<String> = f
            .path
            .iter()
            .map(|c| c.to_string_lossy().into_owned())
            .collect();
        let Some((name, dirs)) = comps.split_last() else {
            continue;
        };
        let mut d = &mut root;
        for c in dirs {
            d = d.dirs.entry(c.clone()).or_default();
        }
        d.files.push((name.clone(), i, f.length));
    }
    root.build("")
}

pub enum Purpose {
    /// Not added yet: the download starts when the dialog is confirmed.
    Add {
        torrent_bytes: Vec<u8>,
        download_dir: PathBuf,
    },
    /// Change the selection of a torrent in the list.
    Edit,
}

pub enum Outcome {
    Open,
    Cancel,
    Confirm,
}

pub struct FilesDialog {
    pub purpose: Purpose,
    pub info_hash: InfoHash,
    pub name: String,
    pub selected: Vec<bool>,
    tree: Vec<Node>,
    sizes: Vec<u64>,
    selectable: Vec<usize>,
    collapsed: HashSet<String>,
}

impl FilesDialog {
    /// `selected` defaults to every file.
    pub fn new(meta: &Metainfo, purpose: Purpose, selected: Option<Vec<bool>>) -> Self {
        let n = meta.files.len();
        let selected = selected
            .filter(|s| s.len() == n)
            .unwrap_or_else(|| vec![true; n]);
        let selectable = (0..n).filter(|&i| !meta.files[i].pad).collect();
        Self {
            purpose,
            info_hash: meta.info_hash,
            name: meta.name.clone(),
            selected: selected
                .into_iter()
                .zip(&meta.files)
                .map(|(s, f)| s && !f.pad)
                .collect(),
            tree: build_tree(meta),
            sizes: meta.files.iter().map(|f| f.length).collect(),
            selectable,
            collapsed: HashSet::new(),
        }
    }

    pub fn set_all(&mut self, on: bool) {
        for &i in &self.selectable {
            self.selected[i] = on;
        }
    }

    /// `(files, bytes)` selected.
    pub fn summary(&self) -> (usize, u64) {
        self.selectable
            .iter()
            .filter(|&&i| self.selected[i])
            .fold((0, 0), |(n, b), &i| (n + 1, b + self.sizes[i]))
    }

    pub fn any_selected(&self) -> bool {
        self.summary().0 > 0
    }

    /// `live` is the torrent's applied selection and bytes on disk per file;
    /// it adds per-file progress.
    pub fn show(&mut self, ctx: &egui::Context, live: Option<(&[bool], &[u64])>) -> Outcome {
        let mut outcome = Outcome::Open;
        let resp = egui::Modal::new(egui::Id::unique("file-selection")).show(ctx, |ui| {
            ui.set_width(620.0);
            let title = match self.purpose {
                Purpose::Add { .. } => "Choose files to download",
                Purpose::Edit => "Files to download",
            };
            ui.heading(title);
            ui.label(RichText::new(&self.name).strong());
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button("Select all").clicked() {
                    self.set_all(true);
                }
                if ui.button("Select none").clicked() {
                    self.set_all(false);
                }
                let (n, bytes) = self.summary();
                let total: u64 = self.selectable.iter().map(|&i| self.sizes[i]).sum();
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!(
                        "{n} of {} files, {} of {}",
                        self.selectable.len(),
                        fmt::bytes(bytes),
                        fmt::bytes(total)
                    ));
                });
            });
            ui.separator();
            egui::ScrollArea::vertical()
                .max_height(380.0)
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    let mut ctx = RowCtx {
                        selected: &mut self.selected,
                        collapsed: &mut self.collapsed,
                        live,
                    };
                    for node in &self.tree {
                        node_ui(ui, node, 0, &mut ctx);
                    }
                });
            ui.separator();
            if matches!(self.purpose, Purpose::Edit) {
                ui.label(
                    RichText::new("Deselected files keep the data already downloaded.")
                        .weak()
                        .small(),
                );
            }
            ui.horizontal(|ui| {
                let confirm = match self.purpose {
                    Purpose::Add { .. } => "Download",
                    Purpose::Edit => "Apply",
                };
                if ui
                    .add_enabled(self.any_selected(), egui::Button::new(confirm))
                    .on_disabled_hover_text("Select at least one file")
                    .clicked()
                {
                    outcome = Outcome::Confirm;
                }
                if ui.button("Cancel").clicked() {
                    outcome = Outcome::Cancel;
                }
            });
        });
        if resp.should_close() && matches!(outcome, Outcome::Open) {
            outcome = Outcome::Cancel;
        }
        outcome
    }
}

struct RowCtx<'a> {
    selected: &'a mut [bool],
    collapsed: &'a mut HashSet<String>,
    live: Option<(&'a [bool], &'a [u64])>,
}

const INDENT: f32 = 18.0;

fn node_ui(ui: &mut egui::Ui, node: &Node, depth: usize, ctx: &mut RowCtx<'_>) {
    match node {
        Node::Dir {
            id,
            name,
            size,
            files,
            children,
        } => {
            let open = !ctx.collapsed.contains(id);
            ui.horizontal(|ui| {
                ui.add_space(depth as f32 * INDENT);
                let icon = egui::Vec2::splat(ui.spacing().icon_width);
                let (_, arrow) = ui.allocate_exact_size(icon, egui::Sense::click());
                egui::collapsing_header::paint_default_icon(
                    ui,
                    if open { 1.0 } else { 0.0 },
                    &arrow,
                );
                if arrow.clicked() {
                    if open {
                        ctx.collapsed.insert(id.clone());
                    } else {
                        ctx.collapsed.remove(id);
                    }
                }
                let n = files.iter().filter(|&&i| ctx.selected[i]).count();
                let mut all = n == files.len();
                if ui
                    .add(
                        egui::Checkbox::new(&mut all, RichText::new(name).strong())
                            .indeterminate(n > 0 && n < files.len()),
                    )
                    .changed()
                {
                    for &i in files {
                        ctx.selected[i] = all;
                    }
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(RichText::new(fmt::bytes(*size)).weak());
                });
            });
            if open {
                for c in children {
                    node_ui(ui, c, depth + 1, ctx);
                }
            }
        }
        Node::File { name, index, size } => {
            ui.horizontal(|ui| {
                // Line up with directory checkboxes, which follow an arrow.
                ui.add_space(
                    depth as f32 * INDENT + ui.spacing().icon_width + ui.spacing().item_spacing.x,
                );
                ui.checkbox(&mut ctx.selected[*index], name.as_str());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(RichText::new(fmt::bytes(*size)).weak());
                    if let Some((applied, have)) = ctx.live {
                        let applied = applied.get(*index).copied().unwrap_or(false);
                        if applied && ctx.selected[*index] {
                            let done = have.get(*index).copied().unwrap_or(0);
                            let frac = if *size == 0 {
                                1.0
                            } else {
                                (done as f64 / *size as f64) as f32
                            };
                            ui.add(
                                egui::ProgressBar::new(frac)
                                    .desired_width(110.0)
                                    .text(format!("{:.0}%", frac * 100.0)),
                            );
                        } else if !ctx.selected[*index] {
                            ui.label(RichText::new("skipped").weak().small());
                        }
                    }
                });
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use st_client_core::testkit::make_torrent;

    fn meta() -> Metainfo {
        let files: &[(&str, &[u8])] = &[
            ("b/z.bin", &[1; 10]),
            ("readme.txt", &[2; 3]),
            ("a/x.bin", &[3; 20]),
            ("a/deep/y.bin", &[4; 5]),
        ];
        Metainfo::parse(&make_torrent("T", files, 16).0).unwrap()
    }

    #[test]
    fn tree_groups_directories_first_with_sizes() {
        let tree = build_tree(&meta());
        let names: Vec<&str> = tree
            .iter()
            .map(|n| match n {
                Node::Dir { name, .. } | Node::File { name, .. } => name.as_str(),
            })
            .collect();
        assert_eq!(names, ["a", "b", "readme.txt"]);
        let Node::Dir {
            id,
            size,
            files,
            children,
            ..
        } = &tree[0]
        else {
            panic!()
        };
        assert_eq!((id.as_str(), *size), ("a/", 25));
        assert_eq!(files, &[3, 2]);
        assert!(matches!(&children[0], Node::Dir { id, .. } if id == "a/deep/"));
        assert!(matches!(
            &children[1],
            Node::File {
                index: 2,
                size: 20,
                ..
            }
        ));
    }

    #[test]
    fn select_all_none_and_summary() {
        let m = meta();
        let mut d = FilesDialog::new(&m, Purpose::Edit, Some(vec![true, false, false, true]));
        assert_eq!(d.summary(), (2, 15));
        d.set_all(false);
        assert!(!d.any_selected());
        d.set_all(true);
        assert_eq!(d.summary(), (4, 38));
        // A selection of the wrong length falls back to everything.
        let d = FilesDialog::new(&m, Purpose::Edit, Some(vec![false]));
        assert_eq!(d.summary().0, 4);
    }

    #[test]
    fn single_file_torrent() {
        let m = Metainfo::parse(&make_torrent("x.iso", &[("x.iso", &[0; 40])], 16).0).unwrap();
        let tree = build_tree(&m);
        assert_eq!(
            tree,
            vec![Node::File {
                name: "x.iso".into(),
                index: 0,
                size: 40
            }]
        );
    }
}
