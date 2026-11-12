#!/bin/zsh
set -e
cd "$(dirname "$0")"
export PATH="$PATH:/opt/homebrew/bin:/usr/local/bin:/Library/Frameworks/Python.framework/Versions/3.13/bin:/Library/Frameworks/Python.framework/Versions/3.11/bin"

case "${1:-start}" in
  stop|status)
    if [ ! -x .venv/bin/python ]; then
      printf '此目录的后台服务未启动。\n'
      exit 0
    fi
    exec .venv/bin/python scripts/local_service.py "$1"
    ;;
  setup-models)
    if [ "$#" -ne 2 ] || [ ! -f "$2" ]; then
      printf '请填写从 GitHub 发布页下载的 models-v1.zip 路径。用法见 README.md。\n' >&2
      exit 2
    fi
    if ! command -v python3.13 >/dev/null 2>&1; then
      printf '请先安装 Python 3.13，再安装识别模型。\n' >&2
      exit 1
    fi
    if ! python3.13 scripts/fetch_models.py --archive "$2" --destination assets/models; then
      printf '模型包未通过校验。请从 README.md 链接重新下载适用的 ZIP。\n' >&2
      exit 1
    fi
    /bin/sh scripts/setup-slicing-env.sh
    printf '识别依赖已安装。安装 FFmpeg 后，重新启动采集助手。\n'
    exit 0
    ;;
  start)
    if [ "$#" -gt 0 ]; then shift; fi
    ;;
  *)
    printf '用法：%s [start|stop|status|setup-models 模型包路径]\n' "$0" >&2
    exit 2
    ;;
esac

if ! command -v npm >/dev/null 2>&1 || ! node -e '
  const [major, minor] = process.versions.node.split(".").map(Number);
  process.exit(major > 22 || (major === 22 && minor >= 13) ? 0 : 1);
' 2>/dev/null; then
  printf '需要 Node.js 22.13 或更新版本。请按 README.md 安装 Node.js，关闭终端窗口后重新打开。\n' >&2
  exit 1
fi
if ! .venv/bin/python -c 'import pip, sys; assert sys.version_info[:2] == (3, 13)' >/dev/null 2>&1; then
  if ! command -v python3.13 >/dev/null 2>&1; then
    printf '未找到 Python 3.13。请按 README.md 安装 Python 3.13，再次启动。\n' >&2
    exit 1
  fi
  if [[ -L .venv ]]; then
    printf '当前 .venv 是符号链接，无法安全重建。请先移开该链接，再重新启动。\n' >&2
    exit 1
  fi
  printf '正在准备 Python 环境…\n'
  python3.13 -m venv --clear .venv
fi
python_lock=$(/usr/bin/shasum -a 256 requirements.lock.txt)
if [[ $(cat .venv/.requirements-installed 2>/dev/null || true) != "$python_lock" ]] ||
   ! .venv/bin/python -c 'import fastapi, httpx, uvicorn, boto3, PIL' >/dev/null 2>&1; then
  printf '正在安装后端依赖，首次运行需要联网…\n'
  if ! .venv/bin/python -m pip install --disable-pip-version-check -r requirements.lock.txt; then
    printf '后端依赖未装完。请检查上方报错和网络，再重新运行启动程序。\n' >&2
    exit 1
  fi
  printf '%s\n' "$python_lock" > .venv/.requirements-installed
fi
web_lock=$(/usr/bin/shasum -a 256 package.json package-lock.json)
if [[ $(cat node_modules/.requirements-installed 2>/dev/null || true) != "$web_lock" ]] ||
   [[ ! -x node_modules/.bin/vite || ! -x node_modules/.bin/tsc ]] ||
   ! npm ls --depth=0 --include=dev >/dev/null 2>&1; then
  printf '正在安装网页依赖，首次运行需要联网…\n'
  if ! npm ci --include=dev --no-audit --no-fund --fetch-retries=1 --fetch-timeout=30000; then
    printf '网页依赖未装完。请检查上方报错和网络，再重新运行启动程序。\n' >&2
    exit 1
  fi
  printf '%s\n' "$web_lock" > node_modules/.requirements-installed
fi
node scripts/start.mjs --build-only
exec .venv/bin/python scripts/local_service.py start "$@"
