#!/bin/zsh
set -e
cd "$(dirname "$0")"
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

case "${1:-start}" in
  stop|status)
    if [ ! -x .venv/bin/python ]; then
      printf '此目录的后台服务未启动。\n'
      exit 0
    fi
    exec .venv/bin/python scripts/local_service.py "$1"
    ;;
  start)
    if [ "$#" -gt 0 ]; then shift; fi
    ;;
  *)
    printf '用法：%s [start|stop|status]\n' "$0" >&2
    exit 2
    ;;
esac

if ! command -v npm >/dev/null 2>&1; then
  printf '未找到 Node.js/npm，请按 README.md 准备运行环境。\n' >&2
  exit 1
fi
if [ ! -x .venv/bin/python ]; then
  if ! command -v python3.13 >/dev/null 2>&1; then
    printf '未找到 Python 3.13，请按 README.md 准备运行环境。\n' >&2
    exit 1
  fi
  python3.13 -m venv .venv
fi
if ! .venv/bin/python -c 'import fastapi, httpx, uvicorn' >/dev/null 2>&1; then
  .venv/bin/python -m pip install -r requirements.lock.txt
fi
if [ ! -d node_modules ]; then
  npm ci
fi
exec npm start -- "$@"
