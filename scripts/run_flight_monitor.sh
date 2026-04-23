#!/usr/bin/env bash
#
# Run the SADE Flight Monitor using config from a .env file.
#
# Usage:
#   scripts/run_flight_monitor.sh                     # reads <repo>/.env
#   scripts/run_flight_monitor.sh path/to/other.env   # reads a custom env file
#   scripts/run_flight_monitor.sh -h | --help
#
# How it works:
#   1. Resolves the env file path (defaults to <repo-root>/.env).
#   2. Sources the file so every KEY=value becomes an exported env var.
#   3. Runs local pre-flight checks (cert files exist, MQTT_CLIENT_ID set when
#      mTLS is on, TRACKER_FINALIZED_URL set when finalization is on, etc.).
#   4. Prints a one-screen resolved-config summary.
#   5. execs `python run.py` so signals (Ctrl+C / SIGTERM) reach the pipeline
#      directly and no bash shell lingers after launch.
#
# Notes on .env parsing:
#   This script uses `source` to load the env file.  Bash treats `#` as the
#   start of a comment, so values containing `#` (e.g. passwords, some URLs)
#   must be quoted in the .env file:
#     MQTT_PASSWORD="abc#def"
#   Lines starting with `#` and blank lines are ignored by `source`.
#
# This script intentionally does no Docker, no venv bootstrapping, no network
# calls, and no cert content parsing.  It's a thin launcher — if the Python
# environment isn't set up, `exec python run.py` will fail loudly.

set -euo pipefail

# ── Locate the repository root relative to this script ──────────────────────
# Works whether invoked from the repo root, from inside scripts/, or via an
# absolute path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Colour setup (tput-based, falls back to plain text when not a TTY) ──────
if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  COLOR_RESET="$(tput sgr0)"
  COLOR_BOLD="$(tput bold)"
  COLOR_RED="$(tput setaf 1)"
  COLOR_GREEN="$(tput setaf 2)"
  COLOR_YELLOW="$(tput setaf 3)"
  COLOR_CYAN="$(tput setaf 6)"
else
  COLOR_RESET=""
  COLOR_BOLD=""
  COLOR_RED=""
  COLOR_GREEN=""
  COLOR_YELLOW=""
  COLOR_CYAN=""
fi

ok()    { printf "%s[OK]%s %s\n"    "${COLOR_GREEN}"  "${COLOR_RESET}" "$*"; }
warn()  { printf "%s[WARN]%s %s\n"  "${COLOR_YELLOW}" "${COLOR_RESET}" "$*"; }
err()   { printf "%s[ERROR]%s %s\n" "${COLOR_RED}"    "${COLOR_RESET}" "$*"; }
hint()  { printf "%s[HINT]%s %s\n"  "${COLOR_CYAN}"   "${COLOR_RESET}" "$*"; }
header(){ printf "%s%s%s\n"         "${COLOR_BOLD}"   "$*" "${COLOR_RESET}"; }

# ── Usage ────────────────────────────────────────────────────────────────────
usage() {
  cat <<USAGE
Usage: $(basename "$0") [ENV_FILE]

Launch the SADE Flight Monitor using an env file.

Arguments:
  ENV_FILE    Optional path to an env file.  Defaults to "<repo>/.env".

Options:
  -h, --help  Show this help and exit.

Examples:
  $(basename "$0")                      # use ./.env
  $(basename "$0") .env.aws-staging     # use a different env file
  $(basename "$0") /abs/path/to/file    # absolute paths work too

To create your first env file, copy the template:
  cp .env.example .env
  \$EDITOR .env
USAGE
}

# ── Parse args ───────────────────────────────────────────────────────────────
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 1 ]]; then
  err "Too many arguments."
  usage
  exit 2
fi

# Resolve the env file path.  If the argument is a relative path, resolve it
# against the current working directory; if absolute, use it as-is.  If no
# argument is given, default to <repo-root>/.env.
if [[ -n "${1:-}" ]]; then
  if [[ "$1" = /* ]]; then
    ENV_FILE="$1"
  else
    ENV_FILE="$(pwd)/$1"
  fi
else
  ENV_FILE="${REPO_ROOT}/.env"
fi

# ── Verify env file exists ───────────────────────────────────────────────────
if [[ ! -f "${ENV_FILE}" ]]; then
  err "Env file not found: ${ENV_FILE}"
  hint "Create one from the template:"
  printf "        cd %s\n" "${REPO_ROOT}"
  printf "        cp .env.example .env\n"
  printf "        \$EDITOR .env\n"
  exit 1
fi

# ── Load env vars ────────────────────────────────────────────────────────────
# `set -a` auto-exports every variable defined while it's on, so sourcing the
# file puts all KEY=value entries into the environment for the child process.
set -a
# shellcheck disable=SC1090  # dynamic path is intentional
source "${ENV_FILE}"
set +a

# ── Pre-flight checks ────────────────────────────────────────────────────────
# All checks are local (no network, no cert parsing).  Fatal errors abort
# before we launch the pipeline so the engineer sees a clear message rather
# than a hung MQTT CONNECT or a mid-run exception.
CHECK_FAILURES=0

fail_check() {
  err "$*"
  CHECK_FAILURES=$((CHECK_FAILURES + 1))
}

# Normalise TLS flag once so the rest of the script can branch on a plain bool.
MQTT_TLS_ENABLED_LC="$(printf '%s' "${MQTT_TLS_ENABLED:-}" | tr '[:upper:]' '[:lower:]')"
case "${MQTT_TLS_ENABLED_LC}" in
  1|true|yes) TLS_ON=true ;;
  *)          TLS_ON=false ;;
esac

FINALIZE_LC="$(printf '%s' "${FINALIZE_TO_API:-}" | tr '[:upper:]' '[:lower:]')"
case "${FINALIZE_LC}" in
  1|true|yes) FINALIZE_ON=true ;;
  *)          FINALIZE_ON=false ;;
esac

# SESSION_SOURCE_MODE must be one of the supported values.
case "${SESSION_SOURCE_MODE:-}" in
  aws|local) : ;;  # ok
  "")
    fail_check "SESSION_SOURCE_MODE is not set. Expected 'aws' or 'local'."
    ;;
  *)
    fail_check "SESSION_SOURCE_MODE='${SESSION_SOURCE_MODE}' is invalid. Expected 'aws' or 'local'."
    ;;
esac

# When mTLS is on, all three cert paths must be set and the files must exist.
# When mTLS is off, the cert paths are irrelevant and we skip these checks.
MTLS_PATHS_SET_COUNT=0
for v in MQTT_CA_CERT_PATH MQTT_CLIENT_CERT_PATH MQTT_PRIVATE_KEY_PATH; do
  if [[ -n "${!v:-}" ]]; then
    MTLS_PATHS_SET_COUNT=$((MTLS_PATHS_SET_COUNT + 1))
  fi
done

if ${TLS_ON} && [[ "${MTLS_PATHS_SET_COUNT}" -gt 0 && "${MTLS_PATHS_SET_COUNT}" -lt 3 ]]; then
  fail_check "Partial mTLS config — set all three of MQTT_CA_CERT_PATH, MQTT_CLIENT_CERT_PATH, MQTT_PRIVATE_KEY_PATH, or leave all three unset."
fi

if ${TLS_ON} && [[ "${MTLS_PATHS_SET_COUNT}" -eq 3 ]]; then
  for v in MQTT_CA_CERT_PATH MQTT_CLIENT_CERT_PATH MQTT_PRIVATE_KEY_PATH; do
    path="${!v}"
    if [[ ! -f "${path}" ]]; then
      fail_check "${v}='${path}' does not exist or is not a regular file."
    fi
  done
fi

# MQTT_CLIENT_ID is REQUIRED when mTLS is on.  AWS IoT Core policies restrict
# which client IDs a cert may use; paho's random default is silently rejected
# and the MQTT CONNECT hangs forever with no error logged.  Hard-fail here.
if ${TLS_ON} && [[ -z "${MQTT_CLIENT_ID:-}" ]]; then
  fail_check "MQTT_CLIENT_ID is empty but MQTT_TLS_ENABLED=true. AWS IoT Core will silently reject paho's random default — set MQTT_CLIENT_ID in your env file."
fi

# Finalization must know where to POST.
if ${FINALIZE_ON} && [[ -z "${TRACKER_FINALIZED_URL:-}" ]]; then
  fail_check "FINALIZE_TO_API=true but TRACKER_FINALIZED_URL is unset. Set it to the target SADE /tracker-session-finalized URL."
fi

if [[ "${CHECK_FAILURES}" -gt 0 ]]; then
  err "Pre-flight failed with ${CHECK_FAILURES} error(s). Fix the .env file and retry."
  exit 1
fi

# ── Pick Python interpreter ──────────────────────────────────────────────────
VENV_PY="${REPO_ROOT}/venv/bin/python"
if [[ -x "${VENV_PY}" ]]; then
  PYTHON_BIN="${VENV_PY}"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  err "No Python interpreter found. Expected ./venv/bin/python or 'python' on PATH."
  exit 1
fi

# ── Derive a few summary strings (avoid leaking the full finalization URL) ──
TRACKER_DISPLAY="(disabled)"
if ${FINALIZE_ON}; then
  # Extract host only from TRACKER_FINALIZED_URL for display — keeps logs
  # from carrying the full path when scanned or pasted.
  tracker_host="$(printf '%s' "${TRACKER_FINALIZED_URL}" \
    | sed -E 's#^[a-zA-Z]+://([^/]+).*#\1#')"
  TRACKER_DISPLAY="enabled → ${tracker_host}"
fi

if ${TLS_ON}; then
  if [[ "${MTLS_PATHS_SET_COUNT}" -eq 3 ]]; then
    TRANSPORT_LABEL="mTLS"
  else
    TRANSPORT_LABEL="TLS (system CA bundle)"
  fi
else
  TRANSPORT_LABEL="plain TCP"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
printf "\n"
header "SADE Flight Monitor — pre-flight summary"
printf -- "----------------------------------------\n"
printf "Env file       : %s\n\n" "${ENV_FILE}"

printf "MQTT broker    : %s:%s\n" "${MQTT_BROKER_HOST:-}" "${MQTT_BROKER_PORT:-}"
printf "MQTT topic     : %s\n"    "${MQTT_TOPIC:-}"
printf "MQTT client ID : %s\n"    "${MQTT_CLIENT_ID:-(unset)}"
printf "MQTT transport : %s\n"    "${TRANSPORT_LABEL}"

if [[ "${MTLS_PATHS_SET_COUNT}" -eq 3 ]]; then
  printf "  CA cert      : %s  [%s]\n" "${MQTT_CA_CERT_PATH}"     "$( [[ -f "${MQTT_CA_CERT_PATH}" ]]     && printf 'OK' || printf 'MISSING' )"
  printf "  Client cert  : %s  [%s]\n" "${MQTT_CLIENT_CERT_PATH}" "$( [[ -f "${MQTT_CLIENT_CERT_PATH}" ]] && printf 'OK' || printf 'MISSING' )"
  printf "  Private key  : %s  [%s]\n" "${MQTT_PRIVATE_KEY_PATH}" "$( [[ -f "${MQTT_PRIVATE_KEY_PATH}" ]] && printf 'OK' || printf 'MISSING' )"
fi

printf "\n"
printf "Session mode   : %s\n" "${SESSION_SOURCE_MODE:-}"
printf "Finalization   : %s\n" "${TRACKER_DISPLAY}"
printf "Log level      : %s\n" "${LOG_LEVEL:-INFO}"

printf "\n"
printf "Python         : %s\n" "${PYTHON_BIN}"

printf "\n"
ok "Pre-flight checks passed."
printf "Launching pipeline (Ctrl+C to stop) ...\n\n"

# ── Launch ───────────────────────────────────────────────────────────────────
# `exec` replaces this shell with the Python process, so Ctrl+C and SIGTERM
# hit Python directly and no shell stays around to eat signals.
cd "${REPO_ROOT}"
exec "${PYTHON_BIN}" run.py
