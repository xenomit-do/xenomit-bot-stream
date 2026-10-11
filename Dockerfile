FROM rust:1.89-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --no-create-home --uid 10001 stream
COPY --from=build /src/target/release/xenomit-bot-stream /usr/local/bin/xenomit-bot-stream
USER 10001
EXPOSE 8090 8091
ENTRYPOINT ["/usr/local/bin/xenomit-bot-stream"]
