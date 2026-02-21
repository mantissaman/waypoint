//! Self-update mechanism via the GitHub Releases API.
//! Downloads platform-specific binaries and performs atomic
//! in-place replacement, with a fallback to install.sh.

use std::env;
use std::fs;
use std::io::Read;
use std::process::Command;

use colored::Colorize;
use flate2::read::GzDecoder;
use semver::Version;
use tar::Archive;
use waypoint_core::error::WaypointError;

const REPO: &str = "tensorbee/waypoint";
const INSTALL_SH_URL: &str = "https://raw.githubusercontent.com/tensorbee/waypoint/main/install.sh";

/// Minimal representation of a GitHub release for version checking.
#[derive(serde::Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

/// Parse the compile-time crate version into a semver Version.
fn current_version() -> Result<Version, WaypointError> {
    Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse current version: {e}")))
}

/// Fetch the latest release metadata from the GitHub API.
fn fetch_latest_release() -> Result<GitHubRelease, WaypointError> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let resp: GitHubRelease = ureq::get(&url)
        .header("User-Agent", "waypoint-self-update")
        .call()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to fetch latest release: {e}")))?
        .body_mut()
        .read_json()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse release JSON: {e}")))?;
    Ok(resp)
}

/// Parse a GitHub release tag (with optional 'v' prefix) into a semver Version.
fn parse_version(tag: &str) -> Result<Version, WaypointError> {
    let v = tag.strip_prefix('v').unwrap_or(tag);
    Version::parse(v)
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse version '{tag}': {e}")))
}

/// Detect the current OS and architecture for release asset selection.
fn platform_target() -> Result<(&'static str, &'static str), WaypointError> {
    let os = match env::consts::OS {
        "linux" => "linux",
        "macos" => "macos",
        _ => {
            return Err(WaypointError::UpdateError(format!(
                "Unsupported OS: {}",
                env::consts::OS
            )))
        }
    };
    let arch = match env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => {
            return Err(WaypointError::UpdateError(format!(
                "Unsupported architecture: {}",
                env::consts::ARCH
            )))
        }
    };
    Ok((os, arch))
}

/// Download a release tarball and atomically replace the current binary.
fn download_and_replace(version: &str) -> Result<(), WaypointError> {
    let (os, arch) = platform_target()?;
    let tag = if version.starts_with('v') {
        version.to_string()
    } else {
        format!("v{version}")
    };
    let tarball_name = format!("waypoint-{tag}-{os}-{arch}.tar.gz");
    let url = format!("https://github.com/{REPO}/releases/download/{tag}/{tarball_name}");

    eprintln!("Downloading {}...", url);

    let mut resp = ureq::get(&url)
        .header("User-Agent", "waypoint-self-update")
        .call()
        .map_err(|e| WaypointError::UpdateError(format!("Download failed: {e}")))?;

    let bytes = resp
        .body_mut()
        .read_to_vec()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to read response body: {e}")))?;

    // Extract the binary from the tar.gz
    let gz = GzDecoder::new(&bytes[..]);
    let mut archive = Archive::new(gz);
    let mut binary_data = None;

    for entry in archive
        .entries()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to read tar entries: {e}")))?
    {
        let mut entry =
            entry.map_err(|e| WaypointError::UpdateError(format!("Bad tar entry: {e}")))?;
        let path = entry
            .path()
            .map_err(|e| WaypointError::UpdateError(format!("Bad path in tar: {e}")))?
            .to_path_buf();

        if path.file_name().and_then(|n| n.to_str()) == Some("waypoint") {
            let mut buf = Vec::new();
            entry
                .read_to_end(&mut buf)
                .map_err(|e| WaypointError::UpdateError(format!("Failed to read binary: {e}")))?;
            binary_data = Some(buf);
            break;
        }
    }

    let binary_data = binary_data
        .ok_or_else(|| WaypointError::UpdateError("Binary not found in archive".into()))?;

    // Atomic replace: write to temp file in same directory, then rename
    let current_exe = env::current_exe()
        .map_err(|e| WaypointError::UpdateError(format!("Cannot determine current exe: {e}")))?;
    let exe_dir = current_exe
        .parent()
        .ok_or_else(|| WaypointError::UpdateError("Cannot determine exe directory".into()))?;

    let tmp = tempfile::NamedTempFile::new_in(exe_dir)
        .map_err(|e| WaypointError::UpdateError(format!("Cannot create temp file: {e}")))?;

    fs::write(tmp.path(), &binary_data)
        .map_err(|e| WaypointError::UpdateError(format!("Failed to write new binary: {e}")))?;

    // Set executable permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o755))
            .map_err(|e| WaypointError::UpdateError(format!("Failed to set permissions: {e}")))?;
    }

    // Validate the downloaded binary by running --version on it
    let tmp_path = tmp.into_temp_path();
    let output = std::process::Command::new(AsRef::<std::path::Path>::as_ref(&tmp_path))
        .arg("--version")
        .output();
    match output {
        Ok(o) if o.status.success() => {
            // Binary is valid, proceed with replacement
        }
        _ => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(WaypointError::UpdateError(
                "Downloaded binary failed validation (--version check)".into(),
            ));
        }
    }

    // Create a backup of the current binary before replacing
    let backup_path = current_exe.with_extension("backup");
    if current_exe.exists() {
        fs::copy(&current_exe, &backup_path).map_err(|e| {
            WaypointError::UpdateError(format!("Failed to create backup of current binary: {e}"))
        })?;
    }

    // Persist (disables auto-cleanup) and rename atomically
    if let Err(e) = tmp_path.persist(&current_exe) {
        // Try to restore from backup
        if backup_path.exists() {
            let _ = fs::rename(&backup_path, &current_exe);
        }
        return Err(WaypointError::UpdateError(format!(
            "Failed to replace binary: {e}"
        )));
    }

    // Success — remove backup
    let _ = fs::remove_file(&backup_path);

    Ok(())
}

/// Fall back to the remote install.sh script when direct update fails.
fn fallback_install_sh() -> Result<(), WaypointError> {
    eprintln!("{}", "Falling back to install.sh...".yellow());
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!("curl -sSf {INSTALL_SH_URL} | sh"))
        .status()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to run install.sh: {e}")))?;

    if !status.success() {
        return Err(WaypointError::UpdateError(
            "install.sh exited with non-zero status".into(),
        ));
    }
    Ok(())
}

/// Check for and optionally install the latest waypoint release.
pub fn self_update(check_only: bool, json_output: bool) -> Result<(), WaypointError> {
    let current = current_version()?;
    let release = fetch_latest_release()?;
    let latest = parse_version(&release.tag_name)?;

    if json_output && check_only {
        println!(
            "{}",
            serde_json::json!({
                "current_version": current.to_string(),
                "latest_version": latest.to_string(),
                "update_available": latest > current,
            })
        );
        return Ok(());
    }

    if current >= latest {
        if json_output {
            println!(
                "{}",
                serde_json::json!({
                    "current_version": current.to_string(),
                    "latest_version": latest.to_string(),
                    "update_available": false,
                    "message": "Already up to date.",
                })
            );
        } else {
            eprintln!(
                "{} You are already on the latest version ({}).",
                "✓".green().bold(),
                current
            );
        }
        return Ok(());
    }

    if check_only {
        if !json_output {
            eprintln!(
                "{} Update available: {} → {}",
                "!".yellow().bold(),
                current.to_string().dimmed(),
                latest.to_string().green().bold()
            );
            eprintln!("Run {} to update.", "waypoint self-update".bold());
        }
        return Ok(());
    }

    eprintln!(
        "Updating waypoint {} → {}...",
        current.to_string().dimmed(),
        latest.to_string().green().bold()
    );

    match download_and_replace(&latest.to_string()) {
        Ok(()) => {
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({
                        "current_version": current.to_string(),
                        "latest_version": latest.to_string(),
                        "updated": true,
                        "message": format!("Successfully updated to {}.", latest),
                    })
                );
            } else {
                eprintln!("{} Successfully updated to {}.", "✓".green().bold(), latest);
            }
        }
        Err(e) => {
            eprintln!("{} Direct update failed: {}", "✗".red().bold(), e);
            fallback_install_sh()?;
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({
                        "current_version": current.to_string(),
                        "latest_version": latest.to_string(),
                        "updated": true,
                        "fallback": true,
                        "message": format!("Updated to {} via install.sh.", latest),
                    })
                );
            } else {
                eprintln!("{} Updated via install.sh.", "✓".green().bold());
            }
        }
    }

    Ok(())
}
