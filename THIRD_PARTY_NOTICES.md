# 第三方许可与模型

本包不改变模型算法、预处理、定位数据或阈值。模型是本项目使用者提供的推理资源，校验值见 `assets/models/manifest.json`。仅为本私有项目分发，不额外授予模型再分发或公开使用权。

内置 Python 来自 Astral 的 **python-build-standalone**，采用 CPython 及其各依赖原有许可。上游：<https://github.com/astral-sh/python-build-standalone>；发行说明：<https://docs.astral.sh/python-build-standalone/>。精确解释器版本和上游构建日期记录在 `release.json` 中，Python 许可一并提供。

所有后端及推理依赖的精确版本由两个 requirements 锁定文件记录，分发 wheel 的 SHA-256 记录在 `dependencies.json` 中。FastAPI、React、PyTorch、ONNX Runtime、NumPy、OpenCV、Pillow 等均保留其原许可；各 wheel 附带的 LICENSE、COPYING 和 NOTICE 原文放在本目录，不对这些文件重新授权。

内置 FFmpeg/ffprobe 只使用 FFmpeg 原生组件，关闭外部自动依赖、GPL、nonfree 和网络组件。按 LGPL 2.1 或更新版本分发，完整许可原文一并提供。两个命令共用一个可执行文件，对应的 FFmpeg 源码、命令分派补丁 `ffmpeg-multicall.patch`、构建脚本及 SHA-256 在同一 Release 中提供。

构建脚本见 `scripts/build_ffmpeg.sh`，固定来源及校验值见 `packaging.lock.json`。应用通过独立进程调用视频工具。 [FFmpeg 源码](https://ffmpeg.org/download.html) · [FFmpeg 许可说明](https://ffmpeg.org/legal.html)

OpenCV 上游 wheel 另带 FFmpeg 6.0 动态库（avcodec/avformat 60.3.100、avutil 58.2.100、swscale 7.1.100），其许可保持在 OpenCV 的 LICENSE-3RD-PARTY 中。该版本源码和 OpenCV 的完整构建源码也作为独立 Release 附件提供；固定版本来源见 <https://pypi.org/project/opencv-python-headless/4.10.0.84/#files>。应用抽帧使用前述独立 FFmpeg 8.1.2 工具，模型图像预处理继续使用原 OpenCV 版本。

macOS 系统框架由操作系统提供，不随包复制。应用采用本地临时签名，没有 Developer ID 或公证声明。
