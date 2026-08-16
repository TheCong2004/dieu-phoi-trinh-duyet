//! Extensions that ship inside the app bundle and load into every Wayfern
//! profile, with no per-profile opt-in.
//!
//! These are deliberately NOT `ExtensionManager` records: those are user data
//! (listed in the UI, deletable, synced per account), whereas these are part of
//! the app itself. They also live outside `extensions_dir()/unpacked`, which is
//! wiped on every profile launch.
//!
//! Each bundled zip is unpacked once per version. The marker file next to the
//! unpacked tree holds the hash of the zip it came from, so an app update
//! shipping a new zip re-extracts automatically and an unchanged one is a
//! cheap no-op on every subsequent launch.

use std::fs;
use std::path::{Path, PathBuf};

/// Zips bundled via `tauri.conf.json`'s `bundle.resources`, as
/// (resource file name, unpacked directory name).
const BUNDLED: &[(&str, &str)] = &[("chromex.zip", "chromex")];

/// Marker holding the hash of the zip the unpacked tree was built from.
const HASH_MARKER: &str = ".bundle-hash";

fn resource_zip_path<R: tauri::Runtime>(
  app_handle: &tauri::AppHandle<R>,
  file_name: &str,
) -> Option<PathBuf> {
  use tauri::Manager;
  let dir = app_handle.path().resource_dir().ok()?;
  let path = dir.join("bundled-extensions").join(file_name);
  path.exists().then_some(path)
}

/// Unpack every bundled extension whose zip has changed since the last run.
/// Best-effort: a failure here must never block app startup, so problems are
/// logged and the extension is simply absent from the next launch.
pub fn ensure_unpacked<R: tauri::Runtime>(app_handle: &tauri::AppHandle<R>) {
  for (file_name, dir_name) in BUNDLED {
    let Some(zip_path) = resource_zip_path(app_handle, file_name) else {
      log::warn!("Bundled extension resource '{file_name}' not found, skipping");
      continue;
    };

    if let Err(e) = ensure_one_unpacked(&zip_path, dir_name) {
      log::warn!("Failed to unpack bundled extension '{file_name}': {e}");
    }
  }
}

fn ensure_one_unpacked(
  zip_path: &Path,
  dir_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
  let data = fs::read(zip_path)?;
  let hash = blake3::hash(&data).to_hex().to_string();

  let dest = crate::app_dirs::bundled_extensions_dir().join(dir_name);
  let marker = dest.join(HASH_MARKER);

  // Same zip as last time and the unpacked tree still looks intact.
  if dest.join("manifest.json").exists()
    && fs::read_to_string(&marker).is_ok_and(|existing| existing.trim() == hash)
  {
    return Ok(());
  }

  log::info!("Unpacking bundled extension '{dir_name}'");

  if dest.exists() {
    fs::remove_dir_all(&dest)?;
  }
  fs::create_dir_all(&dest)?;

  unpack_zip(&data, &dest)?;

  if !dest.join("manifest.json").exists() {
    // Without a root manifest Chromium rejects the directory outright, so fail
    // loudly here rather than passing a broken path to --load-extension.
    fs::remove_dir_all(&dest)?;
    return Err(format!("Bundled extension '{dir_name}' has no manifest.json at its root").into());
  }

  // Written last so an interrupted extraction leaves no marker and is retried
  // on the next launch instead of being trusted.
  fs::write(&marker, &hash)?;

  Ok(())
}

fn unpack_zip(data: &[u8], dest: &Path) -> Result<(), Box<dyn std::error::Error>> {
  let mut archive = zip::ZipArchive::new(std::io::Cursor::new(data))?;

  for i in 0..archive.len() {
    let mut file = archive.by_index(i)?;
    // mangled_name() strips absolute paths and `..` components, so a crafted
    // archive cannot write outside dest.
    let out_path = dest.join(file.mangled_name());

    if file.is_dir() {
      fs::create_dir_all(&out_path)?;
      continue;
    }

    if let Some(parent) = out_path.parent() {
      fs::create_dir_all(parent)?;
    }
    let mut out_file = fs::File::create(&out_path)?;
    std::io::copy(&mut file, &mut out_file)?;
  }

  Ok(())
}

/// Directories to append to Chromium's `--load-extension`. Only trees that
/// unpacked cleanly (marker present, manifest present) are returned.
pub fn loadable_paths() -> Vec<String> {
  BUNDLED
    .iter()
    .filter_map(|(_, dir_name)| {
      let dir = crate::app_dirs::bundled_extensions_dir().join(dir_name);
      (dir.join("manifest.json").exists() && dir.join(HASH_MARKER).exists())
        .then(|| dir.to_string_lossy().to_string())
    })
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Write;

  fn zip_with(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
      let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
      for (name, contents) in entries {
        writer
          .start_file::<_, ()>(*name, zip::write::SimpleFileOptions::default())
          .unwrap();
        writer.write_all(contents).unwrap();
      }
      writer.finish().unwrap();
    }
    buf
  }

  #[test]
  fn unpacks_then_skips_identical_zip() {
    let tmp = tempfile::tempdir().unwrap();
    let _guard = crate::app_dirs::set_test_data_dir(tmp.path().to_path_buf());

    let zip_path = tmp.path().join("ext.zip");
    let data = zip_with(&[
      ("manifest.json", br#"{"name":"T","version":"1"}"#.as_slice()),
      ("bg.js", b"1".as_slice()),
    ]);
    fs::write(&zip_path, &data).unwrap();

    ensure_one_unpacked(&zip_path, "t").unwrap();

    let dest = crate::app_dirs::bundled_extensions_dir().join("t");
    assert!(dest.join("manifest.json").exists());
    assert_eq!(fs::read_to_string(dest.join("bg.js")).unwrap(), "1");

    // A file added by hand survives the second call, proving it was skipped
    // rather than re-extracted.
    let sentinel = dest.join("sentinel");
    fs::write(&sentinel, b"x").unwrap();
    ensure_one_unpacked(&zip_path, "t").unwrap();
    assert!(sentinel.exists(), "identical zip should not re-extract");
  }

  #[test]
  fn re_extracts_when_zip_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let _guard = crate::app_dirs::set_test_data_dir(tmp.path().to_path_buf());

    let zip_path = tmp.path().join("ext.zip");
    fs::write(
      &zip_path,
      zip_with(&[("manifest.json", br#"{"version":"1"}"#.as_slice())]),
    )
    .unwrap();
    ensure_one_unpacked(&zip_path, "t").unwrap();

    let dest = crate::app_dirs::bundled_extensions_dir().join("t");
    let stale = dest.join("stale");
    fs::write(&stale, b"x").unwrap();

    fs::write(
      &zip_path,
      zip_with(&[("manifest.json", br#"{"version":"2"}"#.as_slice())]),
    )
    .unwrap();
    ensure_one_unpacked(&zip_path, "t").unwrap();

    assert!(!stale.exists(), "changed zip should wipe the old tree");
    assert!(fs::read_to_string(dest.join("manifest.json"))
      .unwrap()
      .contains("\"2\""));
  }

  #[test]
  fn rejects_zip_without_root_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let _guard = crate::app_dirs::set_test_data_dir(tmp.path().to_path_buf());

    let zip_path = tmp.path().join("ext.zip");
    fs::write(
      &zip_path,
      zip_with(&[("nested/manifest.json", br#"{}"#.as_slice())]),
    )
    .unwrap();

    assert!(ensure_one_unpacked(&zip_path, "t").is_err());
    assert!(!crate::app_dirs::bundled_extensions_dir().join("t").exists());
  }

  #[test]
  fn loadable_paths_ignores_tree_without_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let _guard = crate::app_dirs::set_test_data_dir(tmp.path().to_path_buf());

    // Use a real BUNDLED entry so loadable_paths() would pick it up if the
    // marker check were missing.
    let dest = crate::app_dirs::bundled_extensions_dir().join(BUNDLED[0].1);
    fs::create_dir_all(&dest).unwrap();
    fs::write(dest.join("manifest.json"), b"{}").unwrap();

    assert!(
      loadable_paths().is_empty(),
      "a tree with no hash marker is a partial extraction"
    );

    fs::write(dest.join(HASH_MARKER), b"deadbeef").unwrap();
    assert_eq!(loadable_paths().len(), 1, "marker present makes it loadable");
  }
}
