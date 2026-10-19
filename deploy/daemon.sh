#!/usr/bin/env bash
# SysLume detached supervisor (no root required).
#
# Keeps a long-running service alive after the launching session ends and
# restarts it automatically on crash.  Used on DGX02/DGX03 where the service
# runs as a plain user process (no sudo, therefore no linger and no system
# units).  All state lives under ~/.local/state/syslume/.
#
#   daemon.sh start   <name> <workdir> "<command>"
#   daemon.sh restart <name> <workdir> "<command>"
#   daemon.sh stop    <name>
#   daemon.sh status  <name>
#
# <command> is executed in a supervisor loop with cwd=<workdir>; its stdout
# and stderr append to ~/.local/state/syslume/<name>.log.
set -euo pipefail

STATE="${HOME}/.local/state/syslume"
mkdir -p "${STATE}"

alive() { [ -f "${STATE}/$1.pid" ] && kill -0 "$(cat "${STATE}/$1.pid")" 2>/dev/null; }

write_run_script() { # $1=name  $2=workdir  $3=command
  local name="$1" workdir="$2" cmd="$3" run="${STATE}/$1.run"
  {
    echo '#!/usr/bin/env bash'
    echo "cd \"${workdir}\" || exit 1"
    echo 'while true; do'
    echo "  echo \"[supervisor] ${name} starting \$(date -Is)\""
    echo "  ${cmd}"
    echo "  echo \"[supervisor] ${name} exited rc=\$? at \$(date -Is); restarting in 5s\""
    echo "  sleep 5"
    echo 'done'
  } > "${run}"
  chmod +x "${run}"
}

launch() { # $1=name  $2=workdir  $3=command
  local name="$1" run="${STATE}/$1.run"
  # kill any previous supervisor first (whole process group)
  if [ -f "${STATE}/${name}.pid" ]; then
    local oldpid; oldpid="$(cat "${STATE}/${name}.pid")"
    kill -- -"${oldpid}" 2>/dev/null || kill "${oldpid}" 2>/dev/null || true
    sleep 1
  fi
  write_run_script "${name}" "$2" "$3"
  setsid "${run}" >>"${STATE}/${name}.log" 2>&1 </dev/null &
  local pid=$!
  disown "${pid}" 2>/dev/null || true
  echo "${pid}" > "${STATE}/${name}.pid"
  echo "started ${name} (pid ${pid})"
}

stop() { # $1=name
  local name="$1"
  if alive "${name}"; then
    local pid; pid="$(cat "${STATE}/${name}.pid")"
    kill -- -"${pid}" 2>/dev/null || kill "${pid}" 2>/dev/null || true
    echo "stopped ${name}"
  else
    echo "${name} not running"
  fi
  rm -f "${STATE}/${name}.pid"
}

status() { # $1=name
  local name="$1"
  if alive "${name}"; then
    echo "RUNNING ${name} (pid $(cat "${STATE}/${name}.pid"))"
  else
    echo "STOPPED ${name}"
  fi
  echo "--- last log lines ---"
  tail -n 8 "${STATE}/${name}.log" 2>/dev/null || true
}

case "${1:-}" in
  start)   launch "$2" "$3" "$4" ;;
  restart) shift; launch "$1" "$2" "$3" ;;
  stop)    stop "$2" ;;
  status)  status "$2" ;;
  *) echo "usage: $0 {start|restart} <name> <workdir> \"<command>\" | {stop|status} <name>" >&2; exit 2 ;;
esac
