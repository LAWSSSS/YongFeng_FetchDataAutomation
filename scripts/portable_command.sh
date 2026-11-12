#!/bin/zsh
set -eu

action=${1:-start}
if (( $# )); then shift; fi
case "$action" in
    start|launch) action=launch ;;
    status|stop) ;;
    -h|--help)
        print '用法：zsh 启动采集助手.command [start | status | stop]'
        exit 0
        ;;
    *)
        print -u2 '可用命令：start、status、stop。'
        exit 2
        ;;
esac

folder=${0:A:h}
resources="$folder/运行组件"
python="$resources/runtime/backend/bin/python3.13"
launcher="$resources/application/launcher.py"

if [[ ! -x "$python" || ! -f "$launcher" ]]; then
    print -u2 '找不到内置运行组件。请完整解压，并保留启动命令和“运行组件”在同一文件夹。'
    exit 1
fi

if [[ "$action" == launch && -z "${IVG_PROFILE_DIR:-}" && ! -w "$folder" ]]; then
    print -u2 '程序所在目录不可写。请将整个采集助手文件夹复制到“文稿”等可写位置，再双击启动。'
    exit 1
fi

# The package's data lives beside this command, even when Terminal's current
# directory is elsewhere. Verification may supply an isolated profile.
export IVG_PROFILE_DIR="${IVG_PROFILE_DIR:-$folder/数据/系统}"
exec "$python" -I -B "$launcher" "$action" --output text "$@"
