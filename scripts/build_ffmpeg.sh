#!/bin/sh
# Build two LGPL command names backed by one standalone executable.
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ffmpeg_version=8.1.2
source_archive="$project_dir/build/downloads/ffmpeg-n$ffmpeg_version.tar.gz"
install_dir="${1:-$project_dir/build/ffmpeg-tools}"
mkdir -p "$project_dir/build/downloads"
if [ ! -s "$source_archive" ]; then
    /usr/bin/curl --fail --location --retry 4 --retry-all-errors --connect-timeout 15 \
        --output "$source_archive" "https://github.com/FFmpeg/FFmpeg/archive/refs/tags/n$ffmpeg_version.tar.gz"
fi
# Expected digest is recorded in packaging.lock.json after upstream verification.
"${BUILD_PYTHON:-python3}" "$project_dir/scripts/verify_ffmpeg_source.py" "$source_archive"
mkdir -p "$install_dir/bin"
install_dir=$(CDPATH= cd -- "$install_dir" && pwd)
source_dir=$(mktemp -d "${TMPDIR:-/tmp}/ivg-ffmpeg.XXXXXXXX")
trap 'rm -rf "$source_dir"' EXIT
tar -xf "$source_archive" -C "$source_dir" --strip-components=1
cd "$source_dir"
/usr/bin/patch -F 0 -p1 < "$project_dir/scripts/ffmpeg-multicall.patch"
export MACOSX_DEPLOYMENT_TARGET=14.0
# Do not let FFmpeg mistake the enclosing application Git repository for its own.
export GIT_CEILING_DIRECTORIES="$(dirname "$source_dir")"
./configure --prefix=/ --arch=arm64 --target-os=darwin --cc=/usr/bin/clang \
    --disable-autodetect --disable-gpl --disable-nonfree --disable-shared --enable-static \
    --disable-network --disable-doc --disable-debug --disable-avdevice --disable-ffplay \
    --extra-cflags=-mmacosx-version-min=14.0 --extra-ldflags=-mmacosx-version-min=14.0
make -j "$(/usr/sbin/sysctl -n hw.ncpu)" ffmpeg
/usr/bin/install -m 755 ffmpeg "$install_dir/bin/ffmpeg"
ln -sfn ffmpeg "$install_dir/bin/ffprobe"
cp COPYING.LGPLv2.1 "$install_dir/FFmpeg-LICENSE.txt"
cp ffbuild/config.log "$install_dir/build-config.log"
