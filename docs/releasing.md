# 可复现的 Mac 完整包

目标：Apple 芯片 Mac，最低macOS14。构建需要 Xcode 命令行工具、Node22.13+、Python3.13及3.11；最终使用者不需要开发环境。

## 构建输入

- `packaging.lock.json`：独立 Python 发行版、FFmpeg及对应源码版本。
- `requirements*.lock.txt`、`assets/wheels.lock.json`：依赖版本、完整 wheel 名称及 SHA-256。构建器拒绝多余、缺失或摘要不符的 wheel。
- `assets/models/manifest.json`：两个模型、定位文件与推理源码的精确大小和 SHA-256。模型版本未改变。
- 实际工作树：包含已跟踪和未忽略的新源码模块，按文件类型白名单导出；数据库、凭据、媒体、缓存和未知文件不进入源码包。

禁止直接复制开发用 `.venv`、`.venv-slicing` 或用户数据到分发包。

## 顺序

1. 准备锁定的 python-build-standalone 3.13及3.11原始发行版。可使用 `uv python install`，根目录需有匹配的 `BUILD` 标记；构建时核对补丁版本。
2. 下载依赖到 `build/wheels/backend` 和 `build/wheels/inference`：

   ```bash
   .venv/bin/python -m pip download --only-binary=:all: -r requirements.lock.txt -d build/wheels/backend
   .venv-slicing/bin/python -m pip download --only-binary=:all: -r requirements-slicing.lock.txt -d build/wheels/inference
   ```

   若使用离线缓存，只能复用大小及SHA-256与wheel清单完全一致的原始wheel；不能用本机已安装的site-packages代替。

3. 准备模型：

   ```bash
   .venv/bin/python scripts/fetch_models.py --archive /path/to/models-v1.zip --destination build/models
   ```

   没有本地资源包时，使用 `--tag v1.1.0` 获取未变更的模型版本。源码与模型包内的 `assets/models` 也可作为 `--models` 输入。

4. 运行 `sh scripts/build_ffmpeg.sh`，禁用GPL/nonfree/外部自动依赖。可复用以前经过完整清单校验的同版本独立FFmpeg二进制及许可证；对应源码须重新核对 `packaging.lock.json`。
5. 完成测试及新网页构建，不覆盖运行中的 `dist`：

   ```bash
   npm test
   npm run check
   npx vite build --outDir build/web --emptyOutDir
   .venv/bin/python scripts/scan_secrets.py --history
   ```

6. 在停止修改代码后构建，使用默认的本机 Library/Caches 临时目录（不要在 iCloud／同步文件夹内签名）：

   ```bash
   ivg_version="$(.venv/bin/python -c 'from app.runtime import VERSION; print(VERSION)')"
   ivg_package_dir="$HOME/Library/Caches/InspectionVideoGatherBuild/$ivg_version/采集助手-Mac-arm64"
   .venv/bin/python scripts/build_mac.py \
     --backend /path/to/standalone-python-3.13 \
     --inference /path/to/standalone-python-3.11 \
     --output "$ivg_package_dir"
   ```

   从锁定wheel重新安装环境；检查动态库依赖、最低系统版本及签名；附带模型、许可证和完整源码与模型ZIP。构建过程中源码变化会失败，防止应用与源码不一致。

7. 验证完整文件列表、符号链接、执行权限、端口冲突、重复启动、安全停止、中文及空格路径、搬移和两个模型输出：

   ```bash
   .venv/bin/python scripts/verify_mac.py "$ivg_package_dir" \
     --reference-python .venv-slicing/bin/python \
     --reference-models build/models --sample /path/to/local-test-frame.jpg
   .venv/bin/python scripts/scan_secrets.py --directory "$ivg_package_dir"
   ```

   测试使用独立profile，不读取生产登录或新增平台任务。测试照片不打包。

8. 创建交付文件：

   ```bash
   .venv/bin/python scripts/release_assets.py "$ivg_package_dir"
   ```

   版本由 `app/runtime.py` 统一读取。输出完整ZIP、独立源码与模型ZIP、模型ZIP、第三方对应源码和SHA256SUMS；每个附件小于2GiB。发布脚本再次拒绝清单以外文件。使用 `ditto --norsrc` 保留可执行权限与应用结构，避免混入Finder元数据。
9. 在另一目录从最终ZIP解压，重跑完整性检查与启动验证，确认“压缩→解压”没有损坏。核对源码摘要、模型、签名及SHA256SUMS。

## 版本与发布

`release.json` 同时记录版本、唯一包身份、Git基准提交、是否有未提交修改，以及实际源码清单摘要。`sourceDirty=true` 的包属于本地候选包，不声称与远端提交完全一致。`源码与模型.zip` 提供其精确工作树。

只有用户要求提交／发布时才提交、推送或创建Release。发布前整合远端更新，保持私有仓库可见性，不强推；从GitHub回下载后再验证附件。未上传成功不得报告已经发布。

运行包保存数据到应用旁的“数据”目录，默认从空白记录开始。搬移时应用与数据一起保留，按相对位置重新解析受管理路径。构建与测试不修改现有网站任务、算法、识别阈值或人工暂停状态。
