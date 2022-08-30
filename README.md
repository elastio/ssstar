ssstar
------------
ssstar is a Rust library crate as well as a command-line tool to create and extract `tar`-compatible archives containing
objects stored in S3 or S3-compatible storage.  It works similarly to GNU `tar`, and produces archives that are 100%
compatible with `tar`, though it uses different command line arguments.

[![Crates.io](https://img.shields.io/crates/v/ssstar.svg)](https://crates.io/crates/ssstar)
[![Docs.rs](https://docs.rs/ssstar/badge.svg)](https://docs.rs/ssstar)
[![CI](https://github.com/elastio/ssstar/workflows/CI/badge.svg)](https://github.com/elastio/ssstar/actions)
[![Coverage
Status](https://coveralls.io/repos/github/elastio/ssstar/badge.svg?branch=master)](https://coveralls.io/github/elastio/ssstar?branch=master)

## :construction: Under Construction, Experimental

`ssstar` is still under active development and should not yet be used in production.

To install on your local system, use `cargo install`, ie:

```shell
cargo install --git https://github.com/elastio/ssstar.git --rev b9cf76ca3 --locked --force ssstar-cli
```

This uses the most recent commit that @anelson has designated as "stable"-ish.  You can drop the `--rev` argument and
get the latest `master`, but this is more likely to be broken.

After this command succeeds you will have `ssstar-cli` installed locally.  Normally on Linux systems this goes to
`~/.cargo/bin`, so if this isn't in your `PATH` you must add it.

Once you do so, run `ssstar create --help` to see how to perform the archive creation operation.

Here's an example CLI that @anelson uses.  This won't work unless you have configured credentials into the
`anelson-isolated` AWS account, but it's an example of the syntax:

```shell
ssstar create \
	s3://elastio-vault-default-8ibrn2zg6/vault.json \
	s3://elastio-vault-default-8ibrn2zg6/fixed:65536/metadata/chunks/db/ \
	s3://elastio-vault-default-8ibrn2zg6/fixed:65536/metadata/extents/db/ \
	s3://elastio-vault-anelson-test-a045e7928d \
	"s3://elastio-account-level-stack-tfstate42168d51-qjatbtl9y9ve/**" \
	--s3 s3://elastio-vault-default-8ibrn2zg6/test.tar
```

## Installation

### Cargo

* Install the rust toolchain in order to have cargo installed by following
  [this](https://www.rust-lang.org/tools/install) guide.
* run `cargo install ssstar`

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

See [CONTRIBUTING.md](CONTRIBUTING.md).
