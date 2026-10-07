# chameleon_postgis_dev's image, extended to run the exact same GEOS
# version as build/mobilitydb/Dockerfile's MobilityDB image, not just the
# same nominal base image tag. Matching the base image tag alone isn't
# enough: MobilityDB's own Dockerfile runs `apt-get install libgeos-dev`
# to get GEOS headers to compile against, which pulls the newer PGDG-repo
# GEOS package (3.14.1-2.pgdg12+1) rather than the base image's originally
# bundled Debian-bookworm one (3.11.1-1) - confirmed directly: even pinning
# this image to the exact same base image digest MobilityDB's build used
# still reported GEOS 3.9.0/3.11.1 until this same apt-get step was
# replicated here too. See BENCHMARK.md, "GEOS version mismatch" confound.
#
# BASE_IMAGE is a full image reference (tag- or digest-pinned) rather than
# split into separate name/tag ARGs the way build/mobilitydb/Dockerfile
# does - so it can be pointed at an exact digest (e.g.
# imresamu/postgis@sha256:...) when the tag alone isn't reproducible (tags
# aren't immutable - a registry can republish different content under the
# same tag, which is exactly what happened here: a fresh pull of
# imresamu/postgis:17-3.5 returned GEOS 3.11.1 while the digest
# build/mobilitydb's own local image was actually built from returns
# 3.9.0/3.11.1 base + this file's own apt-get bump to 3.14.1 - see
# docker-compose.dev.yml for the exact digest in use). Default is the
# amd64 upstream tag; override for arm64 (e.g. `imresamu/postgis:17-3.5`
# or a pinned digest of it - the same multi-arch fork
# build/mobilitydb/Dockerfile uses for its own arm64 profile).
ARG BASE_IMAGE=postgis/postgis:17-3.5

FROM ${BASE_IMAGE}

# The default (amd64) BASE_IMAGE is bullseye-based, and bullseye's live
# deb.debian.org entries 404 once bullseye ages past its regular
# security-support window - see build/mobilitydb/Dockerfile's own comment
# for the full investigation (snapshot.debian.org pin, debian-security
# being unreachable there regardless of timestamp, explicit exact-version
# downgrades for the packages whose already-installed versions conflict
# with what bullseye's own frozen main/updates archive expects). Applied
# here identically so libgeos-dev resolves against the same source
# MobilityDB's own build uses, landing on the same GEOS version (3.9.0) on
# this same machine - matching versions is what the GEOS-confound fix this
# file exists for actually requires, not any particular absolute version.
# No-op-safe for a non-bullseye BASE_IMAGE override (e.g. arm64's
# imresamu/postgis, bookworm) only in the sense that these exact package
# names/versions are bullseye-specific already - this file, like
# build/mobilitydb/Dockerfile, is not yet made conditional on detecting the
# base OS, so an arm64 build must keep using its own already-working
# apt-get install libgeos-dev path unmodified rather than this one.
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
            libgeos-dev; \
    else \
        apt-get update \
        && apt-get install -y --no-install-recommends libgeos-dev; \
    fi \
    && rm -rf /var/lib/apt/lists/*
