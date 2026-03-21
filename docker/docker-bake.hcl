variable "REPO" {
  default = "pgducklake/pgduckpipe"
}

variable "PG_VERSION" {
  default = "18"
}

variable "DUCKDB_VERSION" {
  default = "v1.5.0"
}

variable "DUCKLAKE_COMMIT" {
  default = "65cace70932f3f68b1a89251f971c903ab3b7781"
}

# Base target: defines build args, target stage, and default tag.
# The docker.yaml workflow overrides tags via `set: pg_duckpipe.tags=...`.
target "pg_duckpipe" {
  dockerfile = "docker/Dockerfile"
  args = {
    PG_VERSION      = "${PG_VERSION}"
    DUCKLAKE_COMMIT = "${DUCKLAKE_COMMIT}"
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

# Standalone daemon image (PG-version-agnostic)
target "duckpipe_daemon" {
  dockerfile = "docker/Dockerfile.daemon"
  args = {
    DUCKDB_VERSION  = "${DUCKDB_VERSION}"
    DUCKLAKE_COMMIT = "${DUCKLAKE_COMMIT}"
  }
  target = "runtime"
  tags   = ["pgducklake/duckpipe-daemon:dev"]
}

target "default" {
  inherits = ["pg_duckpipe_18"]
}
