fn main() {
    // Link libpq for replication connection support
    if let Ok(pg_config_path) = std::env::var("PG_CONFIG") {
        if let Ok(output) = std::process::Command::new(&pg_config_path)
            .arg("--libdir")
            .output()
        {
            let libdir = String::from_utf8_lossy(&output.stdout).trim().to_string();
            println!("cargo:rustc-link-search=native={}", libdir);
        }
    } else {
        // Try default pg_config
        if let Ok(output) = std::process::Command::new("pg_config")
            .arg("--libdir")
            .output()
        {
            let libdir = String::from_utf8_lossy(&output.stdout).trim().to_string();
            println!("cargo:rustc-link-search=native={}", libdir);
        }
    }
    println!("cargo:rustc-link-lib=pq");
}
