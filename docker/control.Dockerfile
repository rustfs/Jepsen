# Copyright 2026 RustFS Team
# Licensed under the Apache License, Version 2.0
#
# Jepsen control node image (Java 21 + Leiningen 2.11.2 + gnuplot + graphviz).

FROM eclipse-temurin:21-jdk-jammy

ENV DEBIAN_FRONTEND=noninteractive
ENV LEIN_ROOT=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      git \
      gnuplot \
      graphviz \
      openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/2.11.2/bin/lein \
    && chmod +x /usr/local/bin/lein \
    && lein version

WORKDIR /jepsen
COPY project.clj /jepsen/project.clj
RUN lein deps
COPY . /jepsen
CMD ["sleep", "infinity"]
