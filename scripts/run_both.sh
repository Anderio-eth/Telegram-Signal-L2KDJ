#!/usr/bin/env bash
# Runs this worker's bots as SEPARATE processes.
#
# Separate processes, not one combined program, is the whole point: Python executes in a single
# thread per process, so a matplotlib render in the signal bot would otherwise block order
# execution in the copy bot. As two processes the OS interleaves them, and a crash in one cannot
# take the other down.
#
# Each is restarted independently if it exits. The copy bot manages real positions, so it must
# come back on its own rather than leaving followers unmirrored until someone notices.
#
# The KDJ signal bot is OFF by default. It drags in matplotlib and numpy, which are the bulk of
# this 512MB worker's memory, and the copy bot — which moves real money on ten accounts — is what
# the service exists for now. Set SIGNAL_BOT_ENABLED=true to bring it back; nothing about it was
# deleted.

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

PIDS=()

# Shut down the whole tree, not just the wrappers.
#
# `kill $wrapper_pid` stops the run_forever loop but leaves the python it launched running as an
# orphan. Two copy bots on one Telegram token means Telegram hands updates to whichever asks
# first — but far worse, both are watching the master and both would mirror the same trade. So the
# python children are killed by name from the array, and the wrappers with them.
shutdown() {
  log "shutting down"
  for pid in "${PIDS[@]}"; do
    # Negative pid = the whole process group started by that wrapper, children included.
    kill -TERM "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
  done
  wait
  exit 0
}

if [ "${SIGNAL_BOT_ENABLED:-false}" = "true" ]; then
  set -m
  run_forever "signal-bot" python -m telegram_signal_k2 &
  PIDS+=("$!")
  set +m
else
  log "signal-bot disabled (set SIGNAL_BOT_ENABLED=true to run it)"
fi

# Job control on, so this wrapper and its python get their own process group and can be signalled
# as one. Without it the kill above reaches the wrapper only.
set -m
run_forever "copy-bot" python -m mexc_copy_bot &
PIDS+=("$!")
set +m

# Forward Render's shutdown signal, so a redeploy stops things cleanly instead of killing the copy
# bot mid-order — and, just as importantly, leaves nothing behind that would keep trading.
trap shutdown SIGTERM SIGINT

wait
