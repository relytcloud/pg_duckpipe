variable "REPO" {
  default = "pgducklake/pgduckpipe"
}

variable "PG_VERSION" {
  default = "18"
}

# Base target: defines build args, target stage, and default tag.
# The docker.yaml workflow overrides tags via `set: pg_duckpipe.tags=...`.
target "pg_duckpipe" {
  args = {
    PG_VERSION = "${PG_VERSION}"
  }
  target = "output"
  tags   = ["${REPO}:${PG_VERSION}-dev"]
}

target "pg_duckpipe_18" {
  inherits = ["pg_duckpipe"]
  args = {
    PG_VERSION = "18"
  }
}

// Standalone daemon image
target "duckpipe_daemon" {
  dockerfile = "Dockerfile.daemon"
  args = {
    PG_VERSION = "${PG_VERSION}"
  }
  target = "runtime"
  tags   = ["pgducklake/duckpipe-daemon:${PG_VERSION}-dev"]
}

target "duckpipe_daemon_18" {
  inherits = ["duckpipe_daemon"]
  args = {
    PG_VERSION = "18"
  }
}

target "default" {
  inherits = ["pg_duckpipe_18"]
}
