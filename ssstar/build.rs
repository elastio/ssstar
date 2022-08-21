use vergen::{vergen, Config};

fn main() {
    // Generate the default 'cargo:' instruction output
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = vergen::ShaKind::Short;
    vergen(config).unwrap()
}
