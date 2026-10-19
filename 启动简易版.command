#!/bin/zsh
set -e
cd "$(dirname "$0")"
if [ ! -x .venv/bin/python ]; then
  python3.13 -m venv .venv
  .venv/bin/python -m pip install -r requirements.lock.txt
fi
if [ ! -d node_modules ]; then
  npm ci
fi
npm start
