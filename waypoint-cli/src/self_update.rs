use std::env;
use std::fs;
use std::io::Read;
use std::process::Command;

use colored::Colorize;
use flate2::read::GzDecoder;
use semver::Version;
use tar::Archive;
use waypoint_core::error::WaypointError;

const REPO: &str = "mantissaman/waypoint";
const INSTALL_SH_URL: &str =
    "https://raw.githubusercontent.com/mantissaman/waypoint/main/install.sh";

#[derive(serde::Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

fn current_version() -> Result<Version, WaypointError> {
    Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse current version: {e}")))
}

async fn fetch_latest_release() -> Result<GitHubRelease, WaypointError> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "waypoint-self-update")
        .send()
        .await
        .map_err(|e| WaypointError::UpdateError(format!("Failed to fetch latest release: {e}")))?;

    if !resp.status().is_success() {
        return Err(WaypointError::UpdateError(format!(
            "GitHub API returned status {}",
            resp.status()
        )));
    }

    resp.json::<GitHubRelease>()
        .await
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse release JSON: {e}")))
}

fn parse_version(tag: &str) -> Result<Version, WaypointError> {
    let v = tag.strip_prefix('v').unwrap_or(tag);
    Version::parse(v)
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse version '{tag}': {e}")))
}

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

async fn download_and_replace(version: &str) -> Result<(), WaypointError> {
    let (os, arch) = platform_target()?;
    let tag = if version.starts_with('v') {
        version.to_string()
    } else {
        format!("v{version}")
    };
    let tarball_name = format!("waypoint-{tag}-{os}-{arch}.tar.gz");
    let url = format!("https://github.com/{REPO}/releases/download/{tag}/{tarball_name}");

    eprintln!("Downloading {}...", url);

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "waypoint-self-update")
        .send()
        .await
        .map_err(|e| WaypointError::UpdateError(format!("Download failed: {e}")))?;

    if !resp.status().is_success() {
        return Err(WaypointError::UpdateError(format!(
            "Download returned status {} for {url}",
            resp.status()
        )));
    }

    let bytes = resp
        .bytes()
        .await
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

    // Persist (disables auto-cleanup) and rename atomically
    let tmp_path = tmp.into_temp_path();
    tmp_path.persist(&current_exe).map_err(|e| {
        WaypointError::UpdateError(format!("Failed to replace binary: {e}"))
    })?;

    Ok(())
}

fn fallback_install_sh() -> Result<(), WaypointError> {
    eprintln!(
        "{}",
        "Falling back to install.sh...".yellow()
    );
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

pub async fn self_update(check_only: bool, json_output: bool) -> Result<(), WaypointError> {
    let current = current_version()?;
    let release = fetch_latest_release().await?;
    let latest = parse_version(&release.tag_name)?;

    if json_output {
        if check_only {
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

    match download_and_replace(&latest.to_string()).await {
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
                eprintln!(
                    "{} Successfully updated to {}.",
                    "✓".green().bold(),
                    latest
                );
            }
        }
        Err(e) => {
            eprintln!(
                "{} Direct update failed: {}",
                "✗".red().bold(),
                e
            );
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
                eprintln!(
                    "{} Updated via install.sh.",
                    "✓".green().bold()
                );
            }
        }
    }

    Ok(())
}
