#!/bin/bash

# ---------------------------------------------------------
# pg_duckpipe install script
#
# This script spins up pg_duckpipe locally via Docker.
# It will:
#   1. Check that Docker is installed and running
#   2. Pull the latest pg_duckpipe image (or reuse an existing one)
#   3. Start a container with a persistent volume
#   4. Wait for Postgres to be ready
#   5. Drop you straight into a psql session
#
# Nothing is installed on your system outside of the Docker
# image and associated volume.
#
# To uninstall, just run: docker rm -f duckpipe && docker volume rm duckpipe_data
# ---------------------------------------------------------

# Exit on subcommand errors
set -Eeuo pipefail

SILENT=false
for arg in "$@"; do
  case "$arg" in
    -y|--yes) SILENT=true ;;
  esac
done

CONTAINER_NAME="${DUCKPIPE_CONTAINER_NAME:-duckpipe}"
VOLUME_NAME="${DUCKPIPE_VOLUME_NAME:-duckpipe_data}"
IMAGE="${DUCKPIPE_IMAGE:-pgducklake/pgduckpipe:18-main}"
PG_USER="${DUCKPIPE_PG_USER:-postgres}"
PG_PORT="${DUCKPIPE_PG_PORT:-15432}"
if [ -z "${DUCKPIPE_PG_PASSWORD:-}" ]; then
  if [ -f /usr/share/dict/words ]; then
    SEED=$(od -An -tu4 -N4 /dev/urandom | tr -d ' ')
    PG_PASSWORD="$(awk -v seed="$SEED" 'BEGIN{srand(seed)} {words[NR]=$1} END{for(i=1;i<=4;i++){idx=int(rand()*NR)+1; w=tolower(words[idx]); printf "%s%s",w,(i<4?"-":"")}}' /usr/share/dict/words)"
  else
    PG_PASSWORD="$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | LC_ALL=C tr -dc 'A-Za-z0-9' | head -c 32)"
  fi
else
  PG_PASSWORD="$DUCKPIPE_PG_PASSWORD"
fi
PG_DATABASE="${DUCKPIPE_PG_DATABASE:-postgres}"

# Color support
if [ -t 1 ] && [ "$(tput colors 2>/dev/null)" -ge 8 ] 2>/dev/null; then
  RED=$'\033[0;31m'
  GREEN=$'\033[0;32m'
  CYAN=$'\033[0;36m'
  BLUE=$'\033[0;34m'
  BOLD=$'\033[1m'
  RESET=$'\033[0m'
else
  RED=''
  GREEN=''
  CYAN=''
  BLUE=''
  BOLD=''
  RESET=''
fi

LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

print_connect_cmd() {
  printf "    %sPGPASSWORD=%s psql -h localhost -p %s -U %s -d %s%s\n" "$CYAN" "$PG_PASSWORD" "$PG_PORT" "$PG_USER" "$PG_DATABASE" "$RESET"
}

run_with_spinner() {
  MSG="$1"
  shift
  "$@" > "$LOG" 2>&1 &
  PID=$!
  i=0
  while kill -0 "$PID" 2>/dev/null; do
    dots=$(( i % 3 + 1 ))
    case $dots in
      1) printf "\r  %s.  " "$MSG" ;;
      2) printf "\r  %s.. " "$MSG" ;;
      3) printf "\r  %s..." "$MSG" ;;
    esac
    i=$(( i + 1 ))
    sleep 0.4
  done
  if wait "$PID"; then
    printf "\r  %s... %sdone!%s\n" "$MSG" "$GREEN" "$RESET"
  else
    printf "\r  %s... %sfailed!%s\n" "$MSG" "$RED" "$RESET"
    echo ""
    printf "  %sError: %s failed.%s Details:\n" "$RED" "$MSG" "$RESET" >&2
    sed 's/^/    /' "$LOG" >&2
    exit 1
  fi
}

printf "%s%s" "$BLUE" "$BOLD"
cat << 'BANNER'

       ____             __   ____  _
      / __ \__  _______/ /__/ __ \(_)___  ___
     / / / / / / / ___/ //_/ /_/ / / __ \/ _ \
    / /_/ / /_/ / /__/ ,< / ____/ / /_/ /  __/
   /_____/\__,_/\___/_/|_/_/   /_/ .___/\___/
                                 /_/

BANNER
printf "%s" "$RESET"
printf "  %sWelcome to pg_duckpipe%s (%shttps://github.com/relytcloud/pg_duckpipe%s)\n" "$BOLD" "$RESET" "$CYAN" "$RESET"
echo ""
echo "  Real-time CDC for PostgreSQL — sync row tables to DuckLake columnar tables."
echo "  Run transactional and analytical workloads in a single database."
echo ""
echo "  This script will:"
echo "    1. Pull the latest pg_duckpipe Docker image (includes pg_duckdb + pg_ducklake)"
echo "    2. Start a container named '$CONTAINER_NAME' with a persistent volume ($VOLUME_NAME)"
echo "    3. Expose PostgreSQL on port $PG_PORT"
echo "    4. Drop you into a psql session"
echo ""
echo "  Nothing is installed on your system outside of Docker."
echo "  To uninstall later: docker rm -f $CONTAINER_NAME && docker volume rm $VOLUME_NAME"
echo ""
echo "  Tip: Run with -y or --yes to skip this prompt next time."
echo ""
printf "  %sIf you find pg_duckpipe useful, a star on GitHub means the world to us:%s\n" "$BOLD" "$RESET"
printf "  %shttps://github.com/relytcloud/pg_duckpipe%s\n" "$CYAN" "$RESET"
echo ""

if [ "$SILENT" = false ]; then
  printf "  Continue? [Y/n] "
  read -r REPLY </dev/tty
  case "$REPLY" in
    [nN]*) echo "Aborted."; exit 0 ;;
  esac
  echo
fi

if ! command -v docker > /dev/null 2>&1; then
  printf "  %sError: Docker is not installed.%s Install it from https://docs.docker.com/get-docker/\n" "$RED" "$RESET" >&2
  exit 1
fi

if ! docker info > /dev/null 2>&1; then
  printf "  %sError: Docker is not running.%s Please start Docker and try again.\n" "$RED" "$RESET" >&2
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
  if docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
    printf "  %spg_duckpipe is already running.%s\n" "$GREEN" "$RESET"
    echo ""
    echo "  To connect, run:"
    print_connect_cmd
    echo ""
  else
    printf "  %sFound existing pg_duckpipe container (stopped).%s\n" "$BOLD" "$RESET"
    echo "  To start it, run: docker start $CONTAINER_NAME"
    echo ""
  fi
  exit 0
else
  if docker volume ls --format '{{.Name}}' | grep -q "^$VOLUME_NAME$"; then
    printf "  %sError: Found existing %s volume, exiting...%s\n" "$RED" "$VOLUME_NAME" "$RESET" >&2
    echo ""
    exit 1
  fi

  run_with_spinner "Pulling pg_duckpipe Docker image" docker pull "$IMAGE"

  run_with_spinner "Starting pg_duckpipe" docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_USER="$PG_USER" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DATABASE" \
    -v "$VOLUME_NAME:/var/lib/postgresql/" \
    -p "$PG_PORT":5432 \
    "$IMAGE"
fi

wait_for_postgres() {
  local retries=0
  local max_retries=10
  until docker exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" -d "$PG_DATABASE" > /dev/null 2>&1; do
    retries=$((retries + 1))
    if [ "$retries" -ge "$max_retries" ]; then
      echo "PostgreSQL did not become ready after ${max_retries} attempts."
      return 1
    fi
    sleep 3
  done
}

run_with_spinner "Waiting for PostgreSQL to be ready" wait_for_postgres

echo ""
printf "  %s%spg_duckpipe is ready!%s\n" "$GREEN" "$BOLD" "$RESET"
echo ""
printf "  Password: %s%s%s\n" "$BOLD" "$PG_PASSWORD" "$RESET"
printf "  %sSave this password -- it won't be shown again.%s\n" "$RED" "$RESET"
echo ""
echo "  To reconnect later, run:"
print_connect_cmd
echo ""
printf "  To connect from another tool, use port %s%s%s (you'll need the password above).\n" "$BOLD" "$PG_PORT" "$RESET"
echo ""
printf "  Get started with the docs: %shttps://github.com/relytcloud/pg_duckpipe#quick-start%s\n" "$CYAN" "$RESET"
echo ""
echo "  Try this quick demo once you're in psql:"
echo ""
printf "    %sCREATE TABLE orders (id BIGSERIAL PRIMARY KEY, customer_id BIGINT, total INT);%s\n" "$CYAN" "$RESET"
printf "    %sINSERT INTO orders(customer_id, total) VALUES (101, 4250), (102, 9900);%s\n" "$CYAN" "$RESET"
printf "    %sSELECT duckpipe.add_table('public.orders');%s\n" "$CYAN" "$RESET"
printf "    %sSELECT * FROM orders_ducklake;  -- columnar copy!%s\n" "$CYAN" "$RESET"
echo ""
printf "  %sLaunching psql...%s\n" "$BOLD" "$RESET"
echo ""
echo ""
docker exec -it "$CONTAINER_NAME" psql -U "$PG_USER" -d "$PG_DATABASE" </dev/tty
