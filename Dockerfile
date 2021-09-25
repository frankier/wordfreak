FROM rust:1.46 AS build

WORKDIR /build
COPY . /build
RUN cargo install --release --path .

FROM debian:bullseye-slim

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get dist-upgrade -yq && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -yq \
    zstd \
    xz-utils \
    git \
    build-essential \
    wget \
    bash \
    coreutils \
    bzip2 \
    ca-certificates \
    curl \
    cmake \
    && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/wordfreak
COPY --from=build /usr/local/cargo/bin/mk_tdmat /usr/bin/
COPY --from=build /usr/local/cargo/bin/mk_disp /usr/bin/

ENV LANG='C.UTF-8' LC_ALL='C.UTF-8'
RUN python3 -m pip install --upgrade poetry==1.1.7
ADD pyproject.toml poetry.lock /opt/wordfreak/
RUN poetry export \
      --without-hashes > requirements.txt && \
    python3 -m pip install -r requirements.txt && \
    rm requirements.txt && \
    rm -rf /root/.cache
RUN ln -sf /usr/bin/python3 /usr/bin/python
ADD workflow /opt/wordfreak/
