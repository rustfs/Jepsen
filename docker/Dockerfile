# Copyright 2026 RustFS Team
# Licensed under the Apache License, Version 2.0
#
# Debian SSH target container for RustFS Jepsen nodes.

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      dpkg \
      iproute2 \
      iptables \
      iputils-ping \
      net-tools \
      openssh-server \
      procps \
      sudo \
      unzip \
      wget \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd /var/lib/rustfs /var/log/rustfs \
    && useradd -m -s /bin/bash jepsen \
    && echo "jepsen ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/jepsen \
    && chmod 0440 /etc/sudoers.d/jepsen \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 22 9000

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
