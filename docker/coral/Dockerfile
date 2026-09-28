# syntax=docker/dockerfile:1
FROM eclipse-temurin:25-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/coral /home/coral \
    && chown -R 10001:10001 /run/coral /home/coral
WORKDIR /opt/coral
# Official v1.4.0 release asset; downloaded binaries never enter the repository.
ADD --checksum=sha256:1e83a383f35b55367a3f337f39ae66c2a6f09c24a58503fe97281beeb4e1863d https://github.com/Coral-Protocol/coral-server/releases/download/v1.4.0/coral-server-1.4.0.jar server.jar
COPY entrypoint.sh healthcheck.sh capture.sh ./
COPY coral-agent.toml /opt/coral/endpoint/coral-agent.toml
RUN chmod 644 server.jar
USER 10001:10001
ENV HOME=/home/coral CONFIG_FILE_PATH=/run/coral/config.toml
EXPOSE 5555
ENTRYPOINT ["/bin/sh", "/opt/coral/entrypoint.sh"]
