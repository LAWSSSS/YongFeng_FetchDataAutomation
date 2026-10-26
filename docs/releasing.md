# 可复现的 Mac 完整包

构建环境为 Apple Silicon Mac，安装开发用 Xcode 命令行工具、Node 22+、Python 和 uv。最终使用者无需这些开发工具。不得将现有 `.venv`、`.venv-slicing`、数据库或平台登录文件复制进包。

1. 使用 `uv python install` 安装 `packaging.lock.json` 指定的 python-build-standalone 3.13 / 3.11 发行包。两个源目录必须有上游 `BUILD` 标识，不能是虚拟环境。
2. 以对应解释器运行 `python -m pip download --only-binary=:all: -r requirements.lock.txt -d build/wheels/backend` 和推理锁定文件到 `build/wheels/inference`。构建器会从 wheel 新建依赖，检查版本依赖，并生成 wheel 校验清单。
3. 运行 `python scripts/fetch_models.py --tag v1.1.0`，或从自己的部署包按 `assets/models/manifest.json` 的精确路径和校验值准备 `build/models`。只取清单中的权重、定位文件和推理源码。
4. 运行 `sh scripts/build_ffmpeg.sh`。下载来源先校验再解压，全部工具以 macOS 14 为最低版本构建；禁用 GPL/nonfree/外部自动依赖。
5. 运行 `npm ci`、`npm run check`、`npx vite build --outDir build/web --emptyOutDir`。使用新目录，不把运行中的 `dist` 或历史网页资源放入新包。
6. 运行 `python scripts/build_mac.py --backend <独立3.13根目录> --inference <独立3.11根目录>`。可用 `--output` 指定全新 staging 目录。该脚本只复制明确清单，不读取运行数据库和登录资料。
7. 运行全部测试以及 `scripts/verify_mac.py`，核验移动、重复打开、端口冲突、停止、模型对照及包内容。`scripts/scan_secrets.py` 检查工作树、Git 历史和解压包；报告只显示路径和类别。
8. 用 macOS `ditto -c -k --sequesterRsrc --keepParent` 创建完整 ZIP。单独生成模型 ZIP，只包含模型清单中的文件。生成 `SHA256SUMS`，把 FFmpeg 对应源码和构建说明一起上传 Release。每个附件须小于 2 GiB。
9. 发布前先获取并整合远端更新、确认私有可见，再提交和正常推送当前分支及 main；不改写历史。回下载全部附件并逐一校验 SHA-256，检查模型清单和远端提交一致。

新包持久数据与凭据名称独立于开发版；默认从空白记录开始。包内不写进度，更新保留使用者自己的独立数据目录。便携路径只覆盖旧包内的默认路径，用户自定义模型及工作区路径不被重写。

原程序可以持续运行。所有测试使用独立 profile、数据库和模拟平台，不启动真实采集、上传或切片任务。
