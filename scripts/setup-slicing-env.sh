#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
python_bin=${PYTHON311:-}
if [ -z "$python_bin" ]; then
  python_bin=$(command -v python3.11 || true)
fi
if [ -z "$python_bin" ] || ! "$python_bin" -c 'import sys; assert sys.version_info[:2] == (3, 11)' >/dev/null 2>&1; then
  echo "未找到可用的 Python 3.11。请安装 Python 3.11 后重新运行。" >&2
  exit 1
fi

cd "$project_dir"
if ! .venv-slicing/bin/python -c 'import pip, sys; assert sys.version_info[:2] == (3, 11)' >/dev/null 2>&1; then
  if [ -L .venv-slicing ]; then
    echo "当前 .venv-slicing 是符号链接，无法安全重建。请先移开该链接，再重试。" >&2
    exit 1
  fi
  "$python_bin" -m venv --clear .venv-slicing
fi
PIP_CACHE_DIR="$project_dir/.cache/pip-slicing" .venv-slicing/bin/python -m pip install --disable-pip-version-check -r requirements-slicing.lock.txt
printf '模型推理环境已安装：%s\n' "$project_dir/.venv-slicing/bin/python"
