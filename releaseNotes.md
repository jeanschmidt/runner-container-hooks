## Features

- The k8s hook only copies a job container's volume sources from inside the work folder, matching the volume-based k8s hook. Other sources fail with `Volume mounts outside of the work folder are not supported`. Symlinked paths are resolved before the check.

## SHA-256 Checksums

- docker: `<DOCKER_SHA>`
- k8s: `<K8S_SHA>`
