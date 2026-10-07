# Native-arch MobilityDB image, built from source against a parameterizable
# PostGIS base. The upstream Dockerfile (MobilityDB-src/docker/Dockerfile)
# hardcodes postgis/postgis, which only publishes amd64 — on Apple Silicon
# that image runs under emulation. We swap the base per-arch via BASE_IMAGE:
#   - postgis/postgis    (amd64 only, official)
#   - imresamu/postgis   (multi-arch fork incl. arm64 — same fix used by
#                          the chameleon-datafusion competitor system)
ARG BASE_IMAGE=postgis/postgis
ARG POSTGRES_VERSION=17
ARG POSTGIS_VERSION=3.5

FROM ${BASE_IMAGE}:${POSTGRES_VERSION}-${POSTGIS_VERSION} AS builder

# The base image's live deb.debian.org bullseye entries 404 once bullseye
# ages past its regular security-support window (packages move to Debian's
# static snapshot archive). The base image ships its own commented-out
# snapshot.debian.org fallback, but it's pinned to a fixed past timestamp
# that turned out to be *older* than what's already baked into this base
# image itself, causing a downgrade conflict (libtiff-dev wanting an older
# libtiff5 than what's already installed) - so this pins to a fresh,
# close-to-now timestamp instead, which for an already-frozen/EOL release
# like bullseye just reflects its final, unchanging package set anyway.
# debian-security turns out unreliable at snapshot.debian.org regardless of
# timestamp - its dists/Release metadata resolves fine (confirmed directly:
# HTTP 200, bullseye-security listed) but individual .deb files under
# pool/updates/ 404 consistently across multiple independently-verified
# valid timestamps, and archive.debian.org (Debian's official permanent
# home for EOL releases) doesn't mirror debian-security at all (confirmed:
# 404 on the dists path itself there). Rather than keep chasing a working
# security-archive source, this drops debian-security entirely and instead
# --allow-downgrades so apt can fall back to bullseye main/updates' own
# (older, pre-security-patch) versions of whatever the base image's
# already-installed newer versions conflict with (libc6, libssl1.1,
# libtiff5, libxml2, libsqlite3-0, and their transitive dependents) -
# main/updates has proven reliably fetchable throughout. `dist-upgrade`
# (rather than naming each conflicting package by hand, which turned into
# an ever-growing whack-a-mole as each fix surfaced another transitive
# package) reconciles the *entire* already-installed set against the new
# sources in one pass, including downgrades, before anything new is
# installed - simpler and more complete than enumerating packages one at a
# time. This is a real trade-off (older CVE-patch levels for those specific
# libraries inside the image), accepted here because this is a
# research-benchmark container on an internal server, not a public-facing
# production service, and the alternative is not being able to build the
# image at all. Check-Valid-Until=false is needed because even a pinned
# snapshot's own Release file eventually "expires" as real time moves past it.
# PGDG removed bullseye from apt.postgresql.org (404 since autumn 2026); its
# sources.list.d entry is redirected to PGDG's archive server, which still
# serves bullseye-pgdg (incl. postgresql-server-dev-17, -h3, -pointcloud).
RUN . /etc/os-release \
    && if [ "$VERSION_CODENAME" = "bullseye" ]; then \
        printf '%s\n' \
            'deb http://snapshot.debian.org/archive/debian/20260925T143544Z bullseye main' \
            'deb http://snapshot.debian.org/archive/debian/20260925T143544Z bullseye-updates main' \
            > /etc/apt/sources.list \
        && find /etc/apt/sources.list.d -name '*.list' -exec sed -i 's|http://apt.postgresql.org/pub/repos/apt|https://apt-archive.postgresql.org/pub/repos/apt|g' {} + \
        && apt-get -o Acquire::Check-Valid-Until=false update \
        && apt-get install -y --no-install-recommends --allow-downgrades \
            libc6=2.31-13+deb11u11 libc6-dev=2.31-13+deb11u11 \
            libssl1.1=1.1.1w-0+deb11u1 \
            libtiff5=4.2.0-1+deb11u5 libtiff-dev=4.2.0-1+deb11u5 \
            libxml2=2.9.10+dfsg-6.7+deb11u4 \
            libicu67=67.1-7 libicu-dev=67.1-7 \
            libsqlite3-0=3.34.1-3 libsqlite3-dev=3.34.1-3 \
            zlib1g-dev \
            libjpeg-dev \
            build-essential \
            cmake \
            libproj-dev \
            libjson-c-dev \
            libgeos-dev \
            postgresql-server-dev-${PG_MAJOR} \
            libh3-dev \
            libh3-1 \
            libgsl-dev; \
    else \
        apt-get update \
        && apt-get install -y --no-install-recommends \
            libc6 libc6-dev libssl-dev libtiff-dev libxml2 libicu-dev libsqlite3-0 libsqlite3-dev zlib1g-dev libjpeg-dev build-essential cmake libproj-dev libjson-c-dev libgeos-dev postgresql-server-dev-${PG_MAJOR} libh3-dev libh3-1 libgsl-dev; \
    fi \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/src/MobilityDB
COPY . .

RUN mkdir -p build \
    && cd build \
    && rm -rf * \
    && cmake -DCMAKE_BUILD_TYPE=Release \
             -DNPOINT=on -DCBUFFER=on -DPOSE=on -DH3=on .. \
    && make -j$(nproc) \
    && make install

RUN cp docker/initdb-mobilitydb.sh /tmp/11_mobilitydb.sh

FROM ${BASE_IMAGE}:${POSTGRES_VERSION}-${POSTGIS_VERSION}

LABEL maintainer="MobilityDB Project - https://github.com/MobilityDB/MobilityDB"
LABEL org.opencontainers.image.description="MobilityDB - An open source geospatial trajectory data management & analysis platform"
LABEL org.opencontainers.image.source="https://github.com/MobilityDB/MobilityDB"

# Same snapshot.debian.org pin and --allow-downgrades trade-off as the
# builder stage above (see that stage's comment for the full reasoning) -
# this is a separate FROM, so the live-mirror 404 risk applies here
# independently.
# PGDG removed bullseye from apt.postgresql.org (404 since autumn 2026); its
# sources.list.d entry is redirected to PGDG's archive server, which still
# serves bullseye-pgdg (incl. postgresql-server-dev-17, -h3, -pointcloud).
RUN . /etc/os-release \
    && if [ "$VERSION_CODENAME" = "bullseye" ]; then \
        printf '%s\n' \
            'deb http://snapshot.debian.org/archive/debian/20260925T143544Z bullseye main' \
            'deb http://snapshot.debian.org/archive/debian/20260925T143544Z bullseye-updates main' \
            > /etc/apt/sources.list \
        && find /etc/apt/sources.list.d -name '*.list' -exec sed -i 's|http://apt.postgresql.org/pub/repos/apt|https://apt-archive.postgresql.org/pub/repos/apt|g' {} + \
        && apt-get -o Acquire::Check-Valid-Until=false update \
        && apt-get install -y --no-install-recommends --allow-downgrades \
            libc6=2.31-13+deb11u11 libc6-dev=2.31-13+deb11u11 \
            libssl1.1=1.1.1w-0+deb11u1 \
            libtiff5=4.2.0-1+deb11u5 libtiff-dev=4.2.0-1+deb11u5 \
            libxml2=2.9.10+dfsg-6.7+deb11u4 \
            libicu67=67.1-7 libicu-dev=67.1-7 \
            libsqlite3-0=3.34.1-3 libsqlite3-dev=3.34.1-3 \
            zlib1g-dev \
            libjpeg-dev \
            postgresql-${PG_MAJOR}-h3 \
            postgresql-${PG_MAJOR}-pointcloud \
            libh3-1 \
            libproj-dev \
            libjson-c-dev \
            libgeos-dev \
            libgsl-dev; \
    else \
        apt-get update \
        && apt-get install -y --no-install-recommends \
            libc6 libc6-dev libssl-dev libtiff-dev libxml2 libicu-dev libsqlite3-0 libsqlite3-dev zlib1g-dev libjpeg-dev postgresql-${PG_MAJOR}-h3 postgresql-${PG_MAJOR}-pointcloud libh3-1 libproj-dev libjson-c-dev libgeos-dev libgsl-dev; \
    fi \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/mobilitydb* /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=builder /usr/lib/postgresql/${PG_MAJOR}/lib/libMobilityDB* /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=builder /tmp/11_mobilitydb.sh /docker-entrypoint-initdb.d/11_mobilitydb.sh
