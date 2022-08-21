use vergen::{vergen, Config};

fn main() {
    // Generate the default 'cargo:' instruction output
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = vergen::ShaKind::Short;
    *config.git_mut().commit_timestamp_mut() = true;
    *config.git_mut(). commit_timestamp_kind_mut() = vergen::TimestampKind::All;
    vergen(config).unwrap()
}
