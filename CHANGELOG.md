# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## Fixes

* The performance of `extract` writing many files to object storage should be improved now.  In general, the `extract`
  speed should more or less match the performance of the `create` operation which produced the archive to be extracted.
* Integration tests are updated to work correctly with the multipart checksum logic in recent MinIO releases.

## Breaking Changes

* Increment MSRV to 1.64

### Dependencies

* Update AWS SDK for S3 from 0.17 to 0.22
* Update AWS SDK common crates like `aws-config`, `aws-http`, etc from 0.47 to 0.52

## 0.2.0 - 2-Sept-2022

### Breaking Changes

* Report average bytes per second rates at the end of each stage of operation.  This modifies the signature of the
  progress callback traits.

## 0.1.3 - 31-Aug-2022

First release published via GitHub Actions.  Functionally identical to 0.1.0.

## 0.1.2 - 31-Aug-2022

Incomplete release missing `ssstar-cli`

## 0.1.1 - 31-Aug-2022

Incomplete release missing `ssstar` and `ssstar-cli`

## 0.1.0 - 31-Aug-2022

Initial release
