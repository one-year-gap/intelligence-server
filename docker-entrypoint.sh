#!/usr/bin/env sh
set -eu

APP_MODE="${APP_MODE:-realtime}"
APP_HOST="${APP_HOST:-0.0.0.0}"
APP_PORT="${APP_PORT:-8000}"

child_pid=""

forward_signal() {
  signal="$1"
  if [ -n "${child_pid}" ] && kill -0 "${child_pid}" 2>/dev/null; then
    kill "-${signal}" "${child_pid}" 2>/dev/null || true
    wait "${child_pid}" || true
  fi
}

on_sigterm() {
  echo "[entrypoint] SIGTERM received. shutting down..."
  forward_signal TERM
  exit 143
}

on_sigint() {
  echo "[entrypoint] SIGINT received. shutting down..."
  forward_signal INT
  exit 130
}

trap on_sigterm TERM
trap on_sigint INT

run_bg_and_wait() {
  "$@" &
  child_pid=$!
  wait "${child_pid}"
  status=$?
  child_pid=""
  return "${status}"
}

run_realtime() {
  echo "[entrypoint] APP_MODE=realtime -> starting recommendation server"
  run_bg_and_wait uvicorn app.realtime.main:app --host "${APP_HOST}" --port "${APP_PORT}"
}

run_batch() {
  echo "[entrypoint] APP_MODE=batch -> running one-off batch"
  run_bg_and_wait python -m app.batch.main
  echo "[entrypoint] batch completed."
}

case "${APP_MODE}" in
  realtime)
    run_realtime
    ;;
  batch)
    run_batch
    ;;
  *)
    echo "[entrypoint] Unknown APP_MODE: ${APP_MODE} (allowed: realtime|batch)"
    exit 2
    ;;
esac
