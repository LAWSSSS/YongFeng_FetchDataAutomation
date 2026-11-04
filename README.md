# 采集助手

按日期查询质检记录，核对采集区间并提交。确认视频的最终区间后，程序自动下载、识别、导出照片并导入指定数据集。任务进度保存在本机。

[快速启动](#快速启动) · [日常使用](#日常使用) · [更正、撤回与重采](#更正撤回与重采) · [状态说明](#状态说明) · [数据与清理](#数据与清理) · [开发验证](#开发与隔离验证) · [Mac 打包](#构建-mac-即用包)

## 快速启动

### 使用 Mac 即用包

适用于 Apple 芯片 Mac、macOS 14 或更新版本。完整解压后双击 **采集助手.app**。包内包含网页、后端、两种识别模型、车斗定位文件、Python 和 FFmpeg，无需另外安装。首次打开如有安全提示，按 macOS 的“隐私与安全性”指引操作。

浏览器自动打开本地网页，默认端口为 8786，占用时自动换用空闲端口。再次双击会打开当前实例。关闭网页后后台继续处理。若即用包支持 `stop`，在解压目录运行以下命令停止后台服务。

```sh
./采集助手.app/Contents/MacOS/Launcher stop
```

带有独立停止入口的旧包可用该入口停止。更新或移动应用前先停止，移动时将旁边的“数据”文件夹一起带上。GitHub 仓库用于获取源码，即用包和模型附件在 Release 单独发布。

[下载已发布的 Mac 包和模型资源](https://github.com/zhaochengxiaohao-design/search_download_upload_auto/releases)

### 从源码启动

源码版需要 Python 3.13 和 Node.js 22.13 或更新版本。双击 **启动简易版.command**，首次运行会安装后端及网页依赖，启动本机后台服务并打开浏览器。

```sh
./启动简易版.command          # 启动或打开正在运行的网页
./启动简易版.command status   # 查询后台状态
./启动简易版.command stop     # 停止，保留任务进度
```

指定其他端口时，在相应命令前加 `PORT=8787`。源码版默认使用 [127.0.0.1:8786](http://127.0.0.1:8786/)，启动日志在 `数据/系统/local-service.log`。

更新源码时，先停止后台服务，备份“数据”目录，再更新代码。依赖锁文件有变化时运行 `npm ci` 和 `.venv/bin/python -m pip install -r requirements.lock.txt`，然后重新启动。

使用源码版的识别功能，还需准备 Python 3.11 推理环境、模型权重和抽帧工具。Apple 芯片 Mac 安装 Xcode 命令行工具后，在项目目录运行以下命令。

```sh
sh scripts/setup-slicing-env.sh
.venv/bin/python scripts/fetch_models.py --tag v1.1.0 --destination assets/models
sh scripts/build_ffmpeg.sh
```

下载私有 Release 的模型需要已登录 GitHub 的 `gh`。也可用 `--archive /路径/models-v1.zip` 替代 `--tag v1.1.0`。模型文件按清单校验，`v1.1.0` 是模型资源版本。源码与模型 ZIP 已包含权重，使用它时可跳过下载。推理环境位于 `.venv-slicing`，工具位于 `build/ffmpeg-tools/bin`。

## 日常使用

1. 打开**设置**，连接质检网站、采集平台和视觉 AI 平台。这里也可以调整下载、切片、照片上传的并行数并查看空间用量。
2. 在**日期采集**选择日期区间，点击“查询待采集视频”。核对原始质检时间和拟采集区间，按需逐条修改，再勾选并确认采集。默认从质检开始后 5 分钟采集 60 分钟，每批最多 20 条。默认准入要求质检时长超过 10 分钟，且有真实车牌或有效智能判级照片。已提交任务可在“采集提交记录”中查询。
3. 在**视频确认**按提交日期展开记录，打开“查看与更正”，点击“查看视频”。查看最终版后勾选确认，再点击“确认最终区间并处理”。各卡片的播放和更正草稿独立，支持同时查看多条。需要批量确认或删除时，使用“批量管理”。
4. 在**处理结果**跟踪下载、识别、照片导出、上传和导入核验。两类照片均导入并通过核验后，任务显示完成。照片入口可查看仍保留在本机的照片包。上传内容为照片包，原片存放在本机。

## 更正、撤回与重采

- **更正区间**。在“查看与更正”里调整开始时间和时长，时长可填 1–120 分钟。每次提交更正都会创建独立版本。
- **撤回本次更正**。最终确认前，打开“重试与版本操作”并点击“撤回本次更正”。程序关闭本版预览并阻止本版进入确认和下载。如果绑定的采集任务仍在运行，程序会先终止它。清理核验通过后，页面恢复到上一有效版本。临时断连或占用解除后会继续核对，若显示需处理，则按详情提示使用“重试撤回”。
- **重新采集同一区间**。在“重试与版本操作”选择“按当前区间重新采集”。确认后创建独立版本，原有版本保留。平台拒绝时，详情会显示原因。

日期查询出现“已有采集”时，使用对应记录或平台核对入口。程序根据任务、相机、名称和起止时间查找唯一匹配项，接回“视频确认”等待预览或确认。如果同一区间有多个候选、原任务缺失或身份不符，可在视频详情查看原因并重新核对。已清理历史的去重键只用于辅助核对。隐藏记录保留隐藏状态。

## 状态说明

| 状态 | 含义 |
| --- | --- |
| 尚未提交采集（`pending`） | 本机已有采集记录，尚无平台任务绑定。若长时间停留，打开“采集提交记录”核对该批是否正在提交、暂停或需要对账。 |
| 平台排队中（`PENDING`） | 已绑定平台任务，平台仍在排队。 |
| 等待平台结果 | 已有采集任务，等待平台生成可用视频。 |
| 撤回处理中 | 更正任务正在终止、清理或等待核对。本版暂停确认和下载。 |
| 待确认 | 视频可以查看或更正，等待用户确认最终区间。 |
| 等待恢复 | 网络、存储或文件占用暂时阻塞处理。恢复后按原进度继续。 |
| 需处理／待核对 | 任务、版本或成果有异常。展开详情查看原因和可用操作。 |
| 可用部分已完成 | 已处理可解码的部分并核验产生的照片，原视频仍有坏位置。 |

## 数据与清理

源码版在项目旁写入“数据”，Mac 即用包在应用旁使用独立的“数据”。运行时的路径由程序管理。

```text
数据/
├── 媒体/       原片、历史版本、照片和 ZIP
├── 临时/       隔离测试等临时资料
├── 系统/       数据库、任务凭证、日志和备份
└── 测试样本/   不参与自动扫描、清理或发布
```

| 操作 | 影响范围 |
| --- | --- |
| 采集提交记录 → 管理记录 → 删除所选记录 | 移除单条明细、整次提交或当天提交的日志。视频、平台任务和后台进度保留。 |
| 视频确认 → 删除记录 | 进行中的记录仅从网页移除，后台继续处理。已结束且符合条件的记录可一并清理已核验的本地文件。 |
| 处理结果 → 清理已结束文件 | 按点击时登记的清单核对版本和占用，再清理符合条件的本地文件。云端成果保留。 |

从处理结果打开“已隐藏的处理中记录”时，可以在详情中再次使用删除操作。登记成功会关闭详情，后台继续处理。如果此时出现了新的更正版本，请核对新版本后重新确认删除。断点、处理中或尚未核验的文件保留至符合清理条件。

备份前停止程序并保留整个数据目录。更换版本时用原来的数据目录恢复记录。登录信息使用本机系统安全存储。仓库与发布包均排除用户数据库、凭据、媒体和测试产物。

## 开发与隔离验证

`app/` 是后端，`src/` 是网页，`tests/` 是测试及模拟平台，`scripts/` 是内部启动、维护和发布工具，`assets/` 保存推理代码和资源校验清单。

```sh
npm test
npm run check
npx vite build --outDir build/web --emptyOutDir
.venv/bin/python scripts/scan_secrets.py --history
```

使用 `build/web` 和临时数据库验证网页。下面两个演练都监听本地 8795 端口，请依次启动。

```sh
RECORD_UI_DIST="$PWD/build/web" RECORD_UI_COUNT=5 \
  .venv/bin/python -m tests.record_management_browser_fixture
```

打开 [127.0.0.1:8795](http://127.0.0.1:8795/)，检查提交记录、多个视频详情、独立播放控件、批量操作和窄屏布局。加上 `RECORD_UI_HIDDEN=1` 可检查隐藏记录删除。使用终端输出的 `hiddenVideoUrl` 打开详情，核对删除后详情关闭、刷新后保持隐藏且后台进度保留。

验证撤回时，启动带有已更正视频的模拟平台。

```sh
RECORD_UI_DIST="$PWD/build/web" RECORD_UI_WITHDRAW=1 \
  .venv/bin/python -m tests.capture_history_browser_fixture
```

先预览更正版，再点击“撤回本次更正”。检查播放器关闭，撤回经过核对后恢复上一版。这一演练会模拟清理响应丢失和平台任务重新运行。真实播放及模型处理使用下方的原片演练。

使用真实原片测试播放及模型处理时运行：

```sh
.venv/bin/python scripts/user_rehearsal.py \
  --root '数据/临时/交互体验' --source /路径/原片.mp4 --port 8790 --records 3
```

交互演练使用独立数据库及模拟采集任务，真实解码、识别并生成照片包。上传与导入锁定关闭。故障注入工具为 `scripts/run_real_video_e2e.py`，参数见 `--help`。

## 构建 Mac 即用包

构建需要 Apple 芯片 Mac、Xcode 命令行工具、Node.js 22.13+、Python 3.13 和 3.11。运行包面向 macOS 14+。解释器、依赖和资源版本以 `packaging.lock.json`、两个依赖锁定文件、`assets/wheels.lock.json` 及模型清单为准。

1. 准备两份锁定的 python-build-standalone 发行版，保留原始 `BUILD` 标记。在两个 Python 环境分别按锁文件下载 wheel：

   ```sh
   .venv/bin/python -m pip download --only-binary=:all: -r requirements.lock.txt -d build/wheels/backend
   .venv-slicing/bin/python -m pip download --only-binary=:all: -r requirements-slicing.lock.txt -d build/wheels/inference
   ```

2. 按源码启动小节准备 FFmpeg；使用 `scripts/fetch_models.py --destination build/models` 下载模型。完成测试及 `build/web` 构建。
3. 组装并检查应用。输出目录应在本机非同步目录下：

   ```sh
   .venv/bin/python scripts/build_mac.py \
     --backend /路径/独立Python3.13 --inference /路径/独立Python3.11 \
     --output /路径/采集助手-Mac-arm64
   .venv/bin/python scripts/verify_mac.py /路径/采集助手-Mac-arm64 \
     --reference-python .venv-slicing/bin/python --reference-models build/models \
     --sample /路径/本地测试帧.jpg
   .venv/bin/python scripts/scan_secrets.py --directory /路径/采集助手-Mac-arm64
   ```

4. 执行 `.venv/bin/python scripts/release_assets.py /路径/采集助手-Mac-arm64` 生成完整 ZIP、源码与模型 ZIP、模型资源、第三方对应源码及校验值。解压 ZIP 后复验启动、停止、搬移与文件校验。

打包使用锁定的 wheel 重新组装两个环境，保留许可和源码清单。安装包的 `release.json` 标识构建使用的提交和工作树，`sourceDirty=true` 表示构建含未提交修改。发布记录与下载文件以 Release 为准。

## 第三方许可与模型

本包不改变模型算法、预处理、定位数据或阈值。模型是本项目使用者提供的推理资源，校验值见 `assets/models/manifest.json`。仅为本私有项目分发，不额外授予模型再分发或公开使用权。

内置 Python 来自 Astral 的 **python-build-standalone**，采用 CPython 及其各依赖原有许可。上游：<https://github.com/astral-sh/python-build-standalone>；发行说明：<https://docs.astral.sh/python-build-standalone/>。精确解释器版本和上游构建日期记录在 `release.json` 中，Python 许可一并提供。

所有后端及推理依赖的精确版本由两个 requirements 锁定文件记录，分发 wheel 的 SHA-256 记录在 `dependencies.json` 中。FastAPI、React、PyTorch、ONNX Runtime、NumPy、OpenCV、Pillow 等均保留其原许可；各 wheel 附带的 LICENSE、COPYING 和 NOTICE 原文放在本目录，不对这些文件重新授权。

内置 FFmpeg/ffprobe 只使用 FFmpeg 原生组件，关闭外部自动依赖、GPL、nonfree 和网络组件。按 LGPL 2.1 或更新版本分发，完整许可原文一并提供。未使用本机 Homebrew 的 FFmpeg 二进制或其依赖库。对应的完整源码压缩包、SHA-256、构建选项和构建脚本在本项目同一 Release 中提供，可用于修改和重新构建这两个独立命令行工具。

FFmpeg 源码：<https://ffmpeg.org/download.html>；许可说明：<https://ffmpeg.org/legal.html>。构建脚本：`scripts/build_ffmpeg.sh`；固定来源及校验值：`packaging.lock.json`。工具通过独立进程调用；应用未与 FFmpeg 库链接。

OpenCV 上游 wheel 另带 FFmpeg 6.0 动态库（avcodec/avformat 60.3.100、avutil 58.2.100、swscale 7.1.100），其许可保持在 OpenCV 的 LICENSE-3RD-PARTY 中。该版本源码和 OpenCV 的完整构建源码也作为独立 Release 附件提供；固定版本来源见 <https://pypi.org/project/opencv-python-headless/4.10.0.84/#files>。应用抽帧使用前述独立 FFmpeg 8.1.2 工具，模型图像预处理继续使用原 OpenCV 版本。

macOS 系统框架由操作系统提供，不随包复制。应用采用本地临时签名，没有 Developer ID 或公证声明。
