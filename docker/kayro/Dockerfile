FROM altinity/clickhouse-server:26.6.4.20001.altinityantalya@sha256:4403168d63f049700a979876a8075a8ae4d7ae6be7ea7e09f4fbc6a526e420fa
ARG SOURCE_COMMIT
ARG BINARY_SHA256
LABEL org.opencontainers.image.source="https://github.com/use-kayro/ClickHouse" \
      org.opencontainers.image.revision=$SOURCE_COMMIT \
      org.opencontainers.image.version="26.6.4.20001-kayro-cas1-rc1" \
      ai.usekayro.clickhouse.binary-sha256=$BINARY_SHA256
COPY clickhouse /usr/bin/clickhouse
RUN echo "$BINARY_SHA256  /usr/bin/clickhouse" | sha256sum -c -
