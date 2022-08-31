use vergen::{vergen, Config};

fn main() {
    // Generate the default 'cargo:' instruction output
    let mut config = Config::default();

    // Git metadata isn't available when publishing the crate, or when it's being compiled
    // from crates.io by `cargo install`, so don't fail if it's not available
    *config.git_mut().skip_if_error_mut() = true;
    *config.git_mut().sha_kind_mut() = vergen::ShaKind::Short;
    *config.git_mut().commit_timestamp_mut() = true;
    *config.git_mut().commit_timestamp_kind_mut() = vergen::TimestampKind::All;

    vergen(config).unwrap();
}
