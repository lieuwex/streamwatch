FROM rust:alpine AS builder

RUN apk update && apk add nodejs

COPY . .
RUN cargo build --release

RUN cd client && npm install .

#####

FROM alpine AS runner

RUN mkdir -p /opt/streamwatch

COPY --from=builder target/release/streamwatch /opy/streamwatch/streamwatch
COPY --from=builder client/build /opt/streamwatch/build

WORKDIR /opt/streamwatch
CMD ["streamwatch"]
