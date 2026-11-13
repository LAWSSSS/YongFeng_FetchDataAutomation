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
python="$resources/runtime/bin/python3.11"
launcher="$resources/application/launcher.py"

if [[ ! -x "$python" || ! -f "$launcher" ]]; then
    print -u2 '找不到内置运行组件。请完整解压，并保留启动命令和“运行组件”在同一文件夹。'
    exit 1
fi

if [[ "$action" == launch && -z "${IVG_PROFILE_DIR:-}" && ! -w "$folder" ]]; then
    print -u2 '程序所在目录不可写。请将整个采集助手文件夹复制到“文稿”等可写位置，再双击启动。'
    exit 1
fi

# Use only macOS tools until explicitly approved runtime components are ready.
# Pin both inputs so a replaced inventory cannot approve additional components.
helper="$resources/prepare-first-run.js"
manifest="$resources/first-run.json"
helper_hash='@FIRST_USE_HELPER_SHA256@'
manifest_hash='@FIRST_USE_MANIFEST_SHA256@'
verify_input() {
    local filename="$1" expected="$2" actual
    [[ -f "$filename" && ! -L "$filename" && ! -L "$resources" ]] || return 1
    actual=$(/usr/bin/env -i PATH=/usr/bin:/bin /usr/bin/shasum -a 256 "$filename") || return 1
    [[ "${actual%% *}" == "$expected" ]]
}
if ! verify_input "$helper" "$helper_hash" || ! verify_input "$manifest" "$manifest_hash"; then
    print -u2 '首次启动文件校验未通过。请重新下载并完整解压程序包。'
    exit 1
fi
readiness=$(/usr/bin/osascript -l JavaScript "$helper" check "$resources") || exit 1
if [[ "$readiness" == approval-required ]]; then
    if [[ "$action" != launch || ! -t 0 ]]; then
        print -u2 '请先在终端中启动一次，确认本包运行组件：zsh 启动采集助手.command'
        exit 1
    fi
    print '首次使用：是否信任并准备这份采集助手？'
    print '确认后会校验全部组件，并仅移除本包 Python、识别库和视频工具的下载隔离标记。'
    print '请确认它来自可信的项目发布页；不会更改系统安全设置，也不需要管理员密码。'
    print -n '输入 y 后按回车继续，直接回车取消 [y/N]：'
    answer=''
    read -r answer || exit 1
    case "$answer" in
        y|Y|yes|YES) ;;
        *) print '已取消，未修改组件。'; exit 1 ;;
    esac
    print '正在校验并准备组件，请稍候……'
    readiness=$(/usr/bin/osascript -l JavaScript "$helper" approve "$resources") || exit 1
fi
if [[ "$readiness" != ready ]]; then
    print -u2 '运行组件尚未就绪，请重新解压完整包后再试。'
    exit 1
fi

# The package's data lives beside this command, even when Terminal's current
# directory is elsewhere. Verification may supply an isolated profile.
export IVG_PROFILE_DIR="${IVG_PROFILE_DIR:-$folder/数据/系统}"
exec "$python" -I -B "$launcher" "$action" --output text "$@"
