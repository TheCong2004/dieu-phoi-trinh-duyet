fn main() {
    #[cfg(target_os = "windows")]
    embed_resource::compile("app.rc", embed_resource::NONE)
        .manifest_required()
        .expect("failed to embed the Windows E2E application manifest");
}
