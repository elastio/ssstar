# Builds a docker image for the `ssstar` CLI, based on debian-slim

####################################################################################################
## Builder
####################################################################################################
FROM rust:1.63-slim-bullseye AS builder

RUN update-ca-certificates

# Create appuser
ENV USER=ssstar
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"


WORKDIR /ssstar

COPY ./ .

RUN cargo build --release -p ssstar-cli

####################################################################################################
## Final image
####################################################################################################
FROM debian:bullseye-slim

# Import from builder.
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

WORKDIR /ssstar

# Copy our build
COPY --from=builder /ssstar/target/release/ssstar ./

# Use an unprivileged user.
USER ssstar:ssstar

CMD ["/ssstar/ssstar"]
