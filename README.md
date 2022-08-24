ssstar
------------
ssstar is a Rust library crate as well as a command-line tool to create and extract `tar`-compatible archives containing
objects stored in S3 or S3-compatible storage.  It works similarly to GNU `tar`, and produces archives that are 100%
compatible with `tar`, though it uses different command line arguments.

[![Build status](https://github.com/elastio/ssstar/workflows/ci/badge.svg)](https://github.com/elastio/ssstar/actions)
[![Crates.io](https://img.shields.io/crates/v/ssstar.svg)](https://crates.io/crates/ssstar)
[![Packaging status](https://repology.org/badge/tiny-repos/ssstar.svg)](https://repology.org/project/ssstar/badges)

Dual-licensed under MIT or Apache-2.0.


## :construction: Under Construction, Experimental

`ssstar` is still under active development and should not yet be used in production.

To install on your local system, use `cargo install`, ie:

```shell
cargo install --git git@github.com:elastio/ssstar.git --rev b4f6aad --frozen --force
```

This uses the most recent commit that @anelson has designated as "stable"-ish.  You can drop the `--rev` argument and
get the latest `master`, but this is more likely to be broken.

