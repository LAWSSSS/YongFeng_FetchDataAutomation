#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
python_bin=${PYTHON311:-}
if [ -z "$python_bin" ]; then
  python_bin=$(command -v python3.11 || true)
fi
if [ -z "$python_bin" ]; then
  echo "未找到 Python 3.11。请安装 Python 3.11 后重新运行。" >&2
  exit 1
fi

cd "$project_dir"
if [ ! -x .venv-slicing/bin/python ]; then
  "$python_bin" -m venv .venv-slicing
fi
PIP_CACHE_DIR="$project_dir/.cache/pip-slicing" .venv-slicing/bin/python -m pip install --upgrade pip
PIP_CACHE_DIR="$project_dir/.cache/pip-slicing" .venv-slicing/bin/python -m pip install -r requirements-slicing.lock.txt
printf '模型推理环境已安装：%s\n' "$project_dir/.venv-slicing/bin/python"
