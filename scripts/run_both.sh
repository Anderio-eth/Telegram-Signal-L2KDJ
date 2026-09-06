#!/usr/bin/env bash
# Runs both bots in one Render worker as SEPARATE processes.
#
# Separate processes, not one combined program, is the whole point: Python executes in a single
# thread per process, so a matplotlib render in the signal bot would otherwise block order
# execution in the copy bot. As two processes the OS interleaves them, and a crash in one cannot
# take the other down.
#
# Each is restarted independently if it exits. The copy bot manages real positions, so it must
# come back on its own rather than leaving followers unmirrored until someone notices.

set -uo pipefail

log() { echo "[run_both] $(date -u +%H:%M:%S) $*"; }

run_forever() {
  local name="$1"
  shift
  local backoff=2
  while true; do
    log "starting $name"
    "$@"
    local code=$?
    # A clean exit still gets restarted: neither bot has a legitimate reason to stop while the
    # worker is up, so exit code 0 means something went wrong quietly.
    log "$name exited with code $code — restarting in ${backoff}s"
    sleep "$backoff"
    backoff=$(( backoff < 60 ? backoff * 2 : 60 ))
  done
}

run_forever "signal-bot" python -m telegram_signal_k2 &
SIGNAL_PID=$!

run_forever "copy-bot" python -m mexc_copy_bot &
COPY_PID=$!

# Forward Render's shutdown signal to both, so a redeploy stops them cleanly instead of being
# killed mid-order.
trap 'log "shutting down"; kill "$SIGNAL_PID" "$COPY_PID" 2>/dev/null; wait; exit 0' SIGTERM SIGINT

wait
