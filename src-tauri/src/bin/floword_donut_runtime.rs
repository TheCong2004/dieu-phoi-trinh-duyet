// Prevents a console window on Windows release builds when Floword owns this
// process as a hidden backend runtime.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
  donutbrowser_lib::run_headless();
}
