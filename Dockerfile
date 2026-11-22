# Shopping Sessions: Rust pipeline.
# Stage 1 compiles a release binary; stage 2 is a minimal Debian runtime with only that binary.
# Build time is never timed by the scorer, so fat LTO + codegen-units=1 cost nothing at run time.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
# The scorers are Cascade Lake or newer. x86-64-v3 (AVX2, BMI2, FMA) is safe there and on any
# Haswell-or-newer laptop; pass --build-arg TARGET_CPU=cascadelake to use AVX-512 as well.
ARG TARGET_CPU=x86-64-v3
ARG TARGETARCH
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN if [ "$TARGETARCH" = "amd64" ] || [ -z "$TARGETARCH" ]; then export RUSTFLAGS="-C target-cpu=$TARGET_CPU"; fi; \
    cargo build --release --locked && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /app/shopping-sessions
# At run time: read-only root, uid 1000, no network, /data read-only, /output and /scratch writable.
# The binary writes nothing but /output and needs no temp files.
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/app/shopping-sessions"]
