# Contribution guidelines

First off, thank you for considering contributing to ssstar.

If your contribution is not straightforward, please first discuss the change you
wish to make by creating a new issue before making the change.

## Reporting issues

Before reporting an issue on the
[issue tracker](https://github.com/elastio/ssstar/issues),
please check that it has not already been reported by searching for some related
keywords.

## Pull requests

Try to do one pull request per change.

### Updating the changelog

Update the changes you have made in
[CHANGELOG](https://github.com/elastio/ssstar/blob/main/CHANGELOG.md)
file under the **Unreleased** section.

Add the changes of your pull request to one of the following subsections,
depending on the types of changes defined by
[Keep a changelog](https://keepachangelog.com/en/1.0.0/):

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

If the required subsection does not exist yet under **Unreleased**, create it!

## Developing

### Set up

This is no different than other Rust projects.

```shell
git clone https://github.com/elastio/ssstar
cd ssstar
cargo test
```

### Useful Commands

- Build and run release version:

  ```shell
  cargo build --release && cargo run --release
  ```

- Run Clippy:

  ```shell
  cargo clippy --all-targets --all-features --workspace
  ```

- Run all tests:

  ```shell
  cargo test --all-features --workspace
  ```

- Check to see if there are code formatting issues

  ```shell
  cargo fmt --all -- --check
  ```

- Format the code in the project

  ```shell
  cargo fmt --all
  ```

## Publishing a Release

This is for Elastio employees with the ability to publish to GitHub and crates.io.

We use the `cargo release` tool to automate most of the release activities.  The important settings are always set up in
the `release.toml`  file.  To perform a release, check out `master` and do the following:

- Make sure you have the latest `master`.
- Make sure all tests are passing on CI
- Update the `CHANGELOG.md` file, changing the `Unreleased` to the version number to be released, and putting a new
  empty `Unreleased` section at the top
- Make sure you have the latest `cargo-release`: `cargo install --force cargo-release`
- Run this command to prepare the release (it won't actually push or release anything):

  ```
  cargo release --workspace --execute --no-publish --no-push $LEVEL
  ```

  Where you replace `$LEVEL` with `major`, `minor`, or `patch` depending on which component of the crate semver you need
  to increment.  For now, while this crate is still pre-1.0, use `patch` for non-breaking changes and `minor` for
  breaking changes.  Do not use `major` without agreement from all contributors and Adam.
- `cargo release` will make two commits in your local repo, one making a new release tagged with `vX.Y.Z` where X, Y,
  and Z are the semver components of the new release, and it will also update `master` with a new `-dev` version that is
  one patch level higher than the released version.
- To actually perform the release, do a `git push --tags` to push all of the changes made by `cargo-release`
- Monitor the progress of the release activites in Github Actions.  Sometimes these fail and then it's a huge PITA since
  you in effect need to undo the release (if the failure happened before the publish to crates.io), or yank the
  crates.io release and make a new patch release which hopefully fixes the problem.

Better ways to do this are welcome.  In particular it would be better if we could automate more of this process.

