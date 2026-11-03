# 采集助手

当前源码版本 **1.3.2**，适用于 Apple 芯片 Mac。已发布安装包见[私有仓库 Releases](https://github.com/zhaochengxiaohao-design/search_download_upload_auto/releases)，附件功能以其版本说明为准；源码更新与安装包发布分别执行。

本机运行的 Python + React 应用：按日期采集视频，人工查看及反复更正，确认最终区间后自动下载、识别、导出照片、上传和导入核验。当前只导入照片，原视频上传保持停用。

## 解压即用

适用 **Apple 芯片 Mac，macOS 14+**。完整包解压后双击 **采集助手.app**，无需安装 Python、Node 或 Homebrew。内含两个识别模型、车斗定位文件、独立运行环境和 FFmpeg／ffprobe，以及 `源码与模型.zip`。

首次使用在“设置”登录平台，再开始日期采集。默认使用本机端口8786；占用时自动选择其他端口。首次打开按 macOS 安全提示确认本应用，不需要关闭系统安全保护。详见 [使用说明](docs/mac-quickstart.md)。

完整包使用应用旁的“数据”目录，从空白记录开始，不读取开发版的数据库或凭据。后续版本沿用用户自行产生的记录。更新前使用旧包的停止入口；不要同时启动两个版本。模型与工具相对应用位置解析，应用文件夹可以改名或搬移；移动时一并保留旁边的“数据”文件夹。

## 日常流程

1. **日期采集**：输入起止日期，点击“查询待采集视频”。先查看质检事实与拟采集区间，时间和时长可以逐条修改，再勾选确认采集，支持批量提交。默认从质检开始后5分钟采集60分钟，每批最多20条。默认准入是“真实车牌或有效智能判级照片，且质检时长严格超过10分钟”，可在设置中调整。
2. **视频确认**：在当前卡片内查看视频，按需要反复修改日期、开始时间及1–120分钟时长；每次更正保留独立版本，最终确认前可撤回当前更正。确认最终区间后，自动登记下载和照片处理。未经确认的新版不进入识别。记录支持筛选、单条及批量删除。已结束且核验完整的项目清理绑定本地文件后移除记录；未结束项目只隐藏，后台继续。
3. **处理结果**：最终确认后即显示下载阶段，后续关联识别、照片导出、上传、导入核验和归档进度。照片排队、暂停与处理中分别显示。完整性明确的解码异常可处理有效片段；环境故障等待恢复，身份和成果冲突保持待核对。照片包在本地画廊查看，已清理内容不从云端自动取回。
4. **手动清理**：处理结束后点击“清理已结束文件”，只清理本次登记且核验充分的下载原片、照片、ZIP和临时文件。云端成果、历史记录及用户原始目录保留；未完成或未经确认的项目不参与。

连接、准入和并行数量集中在 **设置**；媒体、模型和工具位置由程序统一管理。模型阈值、预处理及正常区间算法不因打包改变。两个模型共用抽样，默认30秒；磁盘吊必要时细化到10秒并复用判断。下载与识别、两个照片目标独立调度，保留用户设置的并行上限。

视频卡片按桌面三列、中屏两列、窄屏一列排列；展开、轮询和切换栏目保留更正草稿。设置中的下载、切片进程、照片上传三个栏目在桌面等宽展示。

## 源码运行

开发环境需 Python3.13、Python3.11、Node22.13+ 及 FFmpeg／ffprobe；即用包不需要安装这些工具。

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock.txt
npm ci
npm run build
sh scripts/setup-slicing-env.sh
npm start
```

源码与模型包已经包含 `assets/models/` 内的权重。纯 Git 检出不含权重，按校验清单获取：

```bash
.venv/bin/python scripts/fetch_models.py --tag v1.1.0 --destination assets/models
```

`v1.1.0` 指未变更的模型资源版本，不是当前应用版本。也可传入 `--archive models-v1.zip` 离线获取。源代码默认从相对项目的 `assets/models`（或构建用 `build/models`）寻找模型；解释器使用项目的 `.venv-slicing`，FFmpeg 工具使用 `build/ffmpeg-tools/bin`，可用 `scripts/build_ffmpeg.sh` 构建。

Mac 源码版双击“启动简易版.command”启动后台服务，关闭网页或终端不终止任务；“停止简易版.command”安全停止该开发实例。默认 <http://127.0.0.1:8786/>，可通过 `PORT=8787 npm start` 更换端口。源码版进度位于 `数据/系统/collection.sqlite3`，不要与即用包数据目录混用。

## 检查与构建

```bash
npm test
npm run check
npx vite build --outDir build/web --emptyOutDir
.venv/bin/python scripts/scan_secrets.py --history
```

测试使用隔离数据和模拟平台；开发构建使用 `build/web`，避免覆盖运行中的 `dist`。完整 Mac 包构建、搬移验证及资源校验见 [构建说明](docs/releasing.md)，依赖许可见 [第三方许可](docs/third-party.md)。

页面验证见[界面验收](docs/ui-validation.md)。[真实视频交互体验](docs/interactive-rehearsal.md)提供独立数据库、可批量确认的模拟质检及真实解码和模型处理，上传与导入始终锁定关闭。

- `app/`、`src/`：业务后端与网页。
- `tests/`：当前流程回归、故障注入及模拟平台；不保存测试运行产物。
- `assets/models/`：固定模型清单、推理源码；权重通过分发包提供。
- `scripts/`：启动、打包、完整性与凭据检查。
- `docs/`：[确认后自动处理](docs/final-confirmed-workflow.md)、[工作区](docs/workspace.md)、[记录管理](docs/record-management.md)、[单视频控制与照片包](docs/media-control.md)。

Git 不包含凭据、数据库、模型权重、媒体、依赖和构建产物。完整包额外包含经过校验的模型和运行环境，但同样排除用户数据。打包记录实际源码清单及摘要；未提交的工作树会明确标记，不能当作远端已发布提交。
