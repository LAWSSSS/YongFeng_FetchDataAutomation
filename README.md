# 采集助手

按日期查询质检，核对后提交采集。查看视频并确认最终区间后，程序下载原片、识别画面、导出照片，并上传到指定照片数据集。处理进度保存在本机。

## 快速启动

### 使用 Mac 即用包

适用于 Apple 芯片 Mac、macOS 14 或更新版本。完整解压后双击 **采集助手.app**。包内包含网页、后端、两种识别模型、车斗定位文件、Python 和 FFmpeg，无需另外安装。首次打开如有安全提示，按 macOS 的“隐私与安全性”指引操作。

浏览器自动打开本地网页，默认端口为 8786，占用时自动换用空闲端口。再次双击会打开当前实例。关闭网页后后台继续处理。新版即用包可以在解压目录运行同一个启动程序的停止命令：

```sh
./采集助手.app/Contents/MacOS/Launcher stop
```

已发布的旧包请使用其附带的停止入口。更新或移动应用前先停止，移动时将旁边的“数据”文件夹一起带上。

[下载已发布的 Mac 包和模型资源](https://github.com/zhaochengxiaohao-design/search_download_upload_auto/releases)

### 从源码启动

源码版需要 Python 3.13 和 Node.js 22.13 或更新版本。双击 **启动简易版.command**，首次运行会安装后端及网页依赖，启动本机后台服务并打开浏览器。

```sh
./启动简易版.command          # 启动或打开正在运行的网页
./启动简易版.command status   # 查询后台状态
./启动简易版.command stop     # 停止，保留任务进度
```

指定其他端口时，在相应命令前加 `PORT=8787`。源码版默认使用 [127.0.0.1:8786](http://127.0.0.1:8786/)，启动日志在 `数据/系统/local-service.log`。

首次使用源码版的识别功能，还需准备 Python 3.11 推理环境、模型权重和抽帧工具。在项目目录执行：

```sh
sh scripts/setup-slicing-env.sh
.venv/bin/python scripts/fetch_models.py --tag v1.1.0 --destination assets/models
sh scripts/build_ffmpeg.sh
```

下载私有 Release 的模型需要已登录 GitHub 的 `gh`。也可用 `--archive /路径/models-v1.zip` 替代 `--tag v1.1.0`。模型文件按清单校验，`v1.1.0` 是模型资源版本。源码与模型 ZIP 已包含权重，使用它时可跳过下载。推理环境位于 `.venv-slicing`，工具位于 `build/ffmpeg-tools/bin`。

## 日常使用

1. 在**设置**连接质检网站、采集平台和视觉 AI 平台。下载、切片和照片上传的并行数可分别调整。
2. 在**日期采集**输入日期区间，点击“查询待采集视频”。核对原始质检时间和拟采集区间，必要时逐条修改，再勾选并确认采集。默认从质检开始后 5 分钟采集 60 分钟，每批最多 20 条。默认准入为有真实车牌或有效智能判级照片，且质检时长严格超过 10 分钟。已提交历史统一在“采集提交记录”中查询。
3. 在**视频确认**按提交日期展开记录。同一天的不同批次放在同一个网格中：桌面每行三条，中屏两条，手机一条。不同卡片可以同时打开“查看与更正”，并分别点击“查看视频”。每次更正保留独立版本，最终确认前可撤回当前更正。手动更正时长允许 1–120 分钟。查看最终版后勾选确认，再点击“确认最终区间并处理”。点击“批量管理”可确认或删除所选记录。
4. 在**处理结果**查看下载、识别、照片导出、上传和导入核验。两类照片导入核验结束后才显示完成。点击照片入口可查看本地仍保留的对应照片包。原视频不会随照片上传。
5. 在**设置**查看空间，在**处理结果**按需点击“清理已结束文件”。清理范围以点击时登记的清单为准。

采集提交日志统一从“管理记录”删除：可勾选单条明细、整次提交或当天提交，再点击“删除所选记录”。选择整次提交会合并其中的明细选择。这项操作只移除日志，视频、平台任务和后台进度保留。

需要重做同一时间段时，在“视频确认”的详情内打开“重试与版本操作”，选择“按当前区间重新采集”。确认后创建独立版本，旧版继续保留；平台拒绝时会显示原因。日期查询遇到已有采集，会提供对应记录或平台核对入口。已清理历史的去重键仅供核对，不会单独阻止新采集。

“已有采集”会核对确切任务、相机、名称及起止时间。唯一匹配的任务接回视频确认，采集中的继续等待，完成后可预览并确认处理；不会因此自动下载或重新采集。同一区间有多个候选、原任务缺失或身份不符时显示具体原因，可在视频详情重新核对。本地已登记的视频沿用同一个操作记录，用户隐藏过的记录不会自动恢复到列表。

### 常见状态

| 状态 | 含义 |
| --- | --- |
| 尚未提交采集（`pending`） | 本机已有采集记录，尚无平台任务绑定。若长时间停留，打开“采集提交记录”核对该批是否正在提交、暂停或需要对账。 |
| 平台排队中（`PENDING`） | 已绑定平台任务，平台仍在排队。 |
| 等待平台结果 | 已有采集任务，等待平台生成可用视频。 |
| 待确认 | 视频可以查看或更正，等待用户确认最终区间。 |
| 等待恢复 | 网络、存储或文件占用暂时阻塞处理。恢复后按原进度继续。 |
| 需处理／待核对 | 任务、版本或成果有异常。展开详情查看原因和可用操作。 |
| 可用部分已完成 | 已处理可解码的部分并核验产生的照片，原视频仍有坏位置。 |

## 数据、删除和恢复

源码版在项目旁写入“数据”，Mac 即用包在应用旁使用独立的“数据”。运行时的路径由程序管理。

```text
数据/
├── 媒体/       原片、历史版本、照片和 ZIP
├── 临时/       隔离测试等临时资料
├── 系统/       数据库、任务凭证、日志和备份
└── 测试样本/   不参与自动扫描、清理或发布
```

未完成的记录从网页移除后，后台进度仍保留。符合核验条件的已结束记录可以清理其登记的本地文件。清理会核对版本和使用情况，云端任务、照片成果和未知文件保留。断点、正在处理的文件及未核验照片不会被当成完成品清理。

从处理结果打开“已隐藏的处理中记录”后，仍可在详情中删除记录。删除登记成功后关闭这条详情，后台任务继续；下载进度变化不会阻止删除，新更正版本则需要重新确认。只清理点击时已满足条件的绑定文件，之后才完成的项目不会加入本次清理。

备份前停止程序并保留整个数据目录。更换版本时用原来的数据目录恢复记录。登录信息使用本机系统安全存储。仓库与发布包均排除用户数据库、凭据、媒体和测试产物。

## 开发与隔离验证

`app/` 是后端，`src/` 是网页，`tests/` 是测试及模拟平台，`scripts/` 是内部启动、维护和发布工具，`assets/` 保存推理代码和资源校验清单。

```sh
npm test
npm run check
npx vite build --outDir build/web --emptyOutDir
.venv/bin/python scripts/scan_secrets.py --history
```

验证网页时使用隔离目录和构建，避免覆盖运行中的 `dist`：

```sh
RECORD_UI_DIST="$PWD/build/web" RECORD_UI_COUNT=5 \
  .venv/bin/python -m tests.record_management_browser_fixture
```

打开 [127.0.0.1:8795](http://127.0.0.1:8795/)。检查查询与确认、提交记录、多个视频详情、独立播放控件、批量操作和窄屏布局。此夹具的视频仅用于检查播放器控件。

检查隐藏记录删除时，在上面的夹具启动命令前加 `RECORD_UI_HIDDEN=1`，打开终端输出的 `hiddenVideoUrl`。删除后检查详情关闭、刷新不重新打开、后台状态及文件仍保留。

使用真实原片测试播放及模型处理时运行：

```sh
.venv/bin/python scripts/user_rehearsal.py \
  --root '数据/临时/交互体验' --source /路径/原片.mp4 --port 8790 --records 3
```

交互演练使用独立数据库及模拟采集任务，真实解码、识别并生成照片包。上传与导入锁定关闭。故障注入工具为 `scripts/run_real_video_e2e.py`，参数见 `--help`。

界面共用 `src/ui-system.css` 中的尺寸和状态样式。桌面每行三张视频卡，中屏两张，窄屏一张。各卡片的播放、更正与草稿相互独立。

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
