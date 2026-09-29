#!/bin/sh
# Create private, server-local credentials for a fresh SIMULATE-only install.
# Run once from a checkout. Never print the generated values to a terminal/log.
set -eu
umask 077

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root_env="$repo_dir/.env"
backend_env="$repo_dir/backend_api_python/.env"

if [ -e "$root_env" ] || [ -e "$backend_env" ]; then
    echo "Environment file already exists; refusing to overwrite it." >&2
    exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required." >&2
    exit 1
fi

postgres_password=$(openssl rand -hex 32)
redis_password=$(openssl rand -hex 32)
jobs_password=$(openssl rand -hex 32)
secret_key=$(openssl rand -hex 32)
encryption_key=$(openssl rand -hex 32)
admin_password=$(openssl rand -hex 24)

root_tmp=$(mktemp "$repo_dir/.env.XXXXXXXX")
backend_tmp=$(mktemp "$repo_dir/backend_api_python/.env.XXXXXXXX")
cleanup() {
    rm -f -- "$root_tmp" "$backend_tmp"
}
trap cleanup EXIT HUP INT TERM

{
    printf 'POSTGRES_USER=quantdinger\n'
    printf 'POSTGRES_DB=quantdinger\n'
    printf 'POSTGRES_PASSWORD=%s\n' "$postgres_password"
    printf 'REDIS_PASSWORD=%s\n' "$redis_password"
    printf 'CELERY_REDIS_PASSWORD=%s\n' "$jobs_password"
    printf 'BACKEND_PORT=127.0.0.1:5001\n'
    printf 'FRONTEND_HOST=127.0.0.1\n'
    printf 'FRONTEND_PORT=8888\n'
    printf 'FRONTEND_URL=http://127.0.0.1:8888,http://localhost:8888\n'
    printf 'BUILD_REGION=cn\n'
    printf 'PG_MAX_CONNECTIONS=40\n'
    printf 'PG_SHARED_BUFFERS=64MB\n'
    printf 'REDIS_CACHE_MAXMEMORY=64mb\n'
    printf 'REDIS_JOBS_MAXMEMORY=128mb\n'
    printf 'WORKER_DB_POOL_MIN=1\n'
    printf 'WORKER_DB_POOL_MAX=4\n'
    printf 'TRADING_DB_POOL_MIN=1\n'
    printf 'TRADING_DB_POOL_MAX=4\n'
} > "$root_tmp"

{
    printf 'SECRET_KEY=%s\n' "$secret_key"
    printf 'CREDENTIAL_ENCRYPTION_KEY=%s\n' "$encryption_key"
    printf 'ADMIN_USER=quantdinger\n'
    printf 'ADMIN_PASSWORD=%s\n' "$admin_password"
    printf 'ENABLE_REGISTRATION=false\n'
    printf 'ALLOW_LOCAL_DESKTOP_BROKERS=true\n'
    printf 'FUTU_ALLOW_REMOTE_OPEND=false\n'
} > "$backend_tmp"

chmod 600 "$root_tmp" "$backend_tmp"
mv -n -- "$root_tmp" "$root_env"
mv -n -- "$backend_tmp" "$backend_env"
echo "Created private environment files. Retrieve the admin password directly on the server."
