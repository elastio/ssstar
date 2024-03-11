fn main() {
    let result = vergen::EmitBuilder::builder().cargo_target_triple().emit();

    // Insert vergen info at build time if available
    if let Err(err) = result {
        println!("cargo:warning=Failed to gather build info with `vergen`: {err:#?}",)
    }
}
