#!/bin/sh
# Build redistributable LGPL-only tools; no Homebrew libraries are linked.
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ffmpeg_version=8.1.2
source_archive="$project_dir/build/downloads/ffmpeg-n$ffmpeg_version.tar.gz"
source_dir="$project_dir/build/ffmpeg-source"
install_dir="$project_dir/build/ffmpeg-tools"
mkdir -p "$project_dir/build/downloads"
if [ ! -s "$source_archive" ]; then
    /usr/bin/curl --fail --location --retry 4 --retry-all-errors --connect-timeout 15 \
        --output "$source_archive" "https://github.com/FFmpeg/FFmpeg/archive/refs/tags/n$ffmpeg_version.tar.gz"
fi
# Expected digest is recorded in packaging.lock.json after upstream verification.
"${BUILD_PYTHON:-python3}" "$project_dir/scripts/verify_ffmpeg_source.py" "$source_archive"
mkdir -p "$source_dir" "$install_dir"
tar -xf "$source_archive" -C "$source_dir" --strip-components=1
cd "$source_dir"
export MACOSX_DEPLOYMENT_TARGET=14.0
# Do not let FFmpeg mistake the enclosing application Git repository for its own.
export GIT_CEILING_DIRECTORIES="$project_dir/build"
./configure --prefix=/ --arch=arm64 --target-os=darwin --cc=/usr/bin/clang \
    --disable-autodetect --disable-gpl --disable-nonfree --disable-shared --enable-static \
    --disable-network --disable-doc --disable-debug --disable-avdevice \
    --extra-cflags=-mmacosx-version-min=14.0 --extra-ldflags=-mmacosx-version-min=14.0
make -j "$(/usr/sbin/sysctl -n hw.ncpu)"
make DESTDIR="$install_dir" install
cp COPYING.LGPLv2.1 "$install_dir/FFmpeg-LICENSE.txt"
cp ffbuild/config.log "$install_dir/build-config.log"
