use clap::Parser;
use directories::BaseDirs;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "trein-video")]
#[command(about = "Distributed video converter for NAS", long_about = None)]
pub struct Args {
    /// Path to configuration file. Defaults to the platform config
    /// directory (`~/.config/trein-video/config.toml` on macOS/Linux,
    /// `%APPDATA%/trein-video/config.toml` on Windows) when omitted.
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Instance ID (override config)
    #[arg(short, long)]
    pub instance_id: Option<String>,

    /// Role: master or worker (override config)
    #[arg(short, long)]
    pub role: Option<String>,
}

pub fn parse_args() -> Args {
    Args::parse()
}

/// Resolve the platform-appropriate default config file path, used when
/// `--config` is not passed on the command line.
///
/// Returns `None` if the OS could not resolve a home/base directory (e.g. no
/// `$HOME` set), in which case the caller must fall back to requiring an
/// explicit `--config`.
///
/// Deliberately does **not** use `directories::ProjectDirs` for this: on
/// macOS, `ProjectDirs::config_dir()` resolves to
/// `~/Library/Application Support/trein-video` (Apple's native convention),
/// but this is a terminal/server tool, not a GUI app, so we want the same
/// `~/.config` layout on both Unix platforms. Instead we take `BaseDirs`
/// from the `directories` crate (for correct, cross-platform home/appdata
/// resolution) and build the final path ourselves via the small, unit-tested
/// helpers below.
pub fn default_config_path() -> Option<PathBuf> {
    let base_dirs = BaseDirs::new()?;
    let path = if cfg!(target_os = "windows") {
        windows_config_path(base_dirs.config_dir())
    } else {
        xdg_config_path(base_dirs.home_dir())
    };
    Some(path)
}

/// `%APPDATA%\trein-video\config.toml`, given `appdata_dir` ==
/// `BaseDirs::config_dir()` on Windows (`{FOLDERID_RoamingAppData}`, i.e.
/// `%APPDATA%`).
fn windows_config_path(appdata_dir: &Path) -> PathBuf {
    appdata_dir.join("trein-video").join("config.toml")
}

/// `~/.config/trein-video/config.toml`, given `home_dir` ==
/// `BaseDirs::home_dir()`. Used for both macOS and Linux so the tool behaves
/// the same way on every Unix platform, regardless of each OS's own native
/// config directory convention.
fn xdg_config_path(home_dir: &Path) -> PathBuf {
    home_dir
        .join(".config")
        .join("trein-video")
        .join("config.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windows_config_path_layout() {
        let appdata = PathBuf::from(r"C:\Users\Alice\AppData\Roaming");
        let path = windows_config_path(&appdata);
        assert_eq!(path.file_name().unwrap(), "config.toml");
        assert_eq!(path, appdata.join("trein-video").join("config.toml"));
    }

    #[test]
    fn test_xdg_config_path_layout_macos_and_linux() {
        let home = PathBuf::from("/home/alice");
        let path = xdg_config_path(&home);
        assert_eq!(
            path,
            PathBuf::from("/home/alice/.config/trein-video/config.toml")
        );
    }

    #[test]
    fn test_default_config_path_ends_with_config_toml() {
        if let Some(path) = default_config_path() {
            assert_eq!(path.file_name().unwrap(), "config.toml");
            assert!(path.to_string_lossy().contains("trein-video"));
        }
    }

    #[test]
    fn test_default_config_path_is_absolute_when_resolved() {
        if let Some(path) = default_config_path() {
            assert!(path.is_absolute());
        }
    }

    #[test]
    fn test_default_config_path_uses_xdg_style_on_unix() {
        if !cfg!(target_os = "windows") {
            if let Some(path) = default_config_path() {
                assert!(path.to_string_lossy().contains(".config/trein-video"));
            }
        }
    }
}
