## Features

- Auto-install `python3` via `apt-get` when missing from the job container. If the in-pod `python3 --version` probe fails, the hook now runs `apt-get update && apt-get install -y --no-install-recommends python3` before giving up. This makes the hook work out of the box on stock `ubuntu:24.04` (and other Debian-based images that no longer ship `python3` in the base layer) without users having to rebuild their container image. Non-Debian, non-root, or no-egress images still surface a clear failure with the apt error chained into the "image not compatible" message.

## SHA-256 Checksums

- docker: `<DOCKER_SHA>`
- k8s: `<K8S_SHA>`
