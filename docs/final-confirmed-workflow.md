# 日期采集与最终确认

日常使用只需三步：

1. 在“日期采集”输入起止日期，点击“查询待采集视频”。查询完成后核对区间，时间和时长只逐条修改，勾选后点击“确认采集所选”。默认全天、质检开始后5分钟起采集60分钟；未确认不提交。
2. 在“视频确认与更正”查看视频，可反复修改和重新采集。满意后点击“确认最终区间并处理”，或勾选已经核对的当前版本批量确认。
3. 结果处理结束后，在“自动处理结果”点击“清理已结束文件”。仅处理本次登记的固定范围，后来完成的项目留到下次。

确认后自动下载、识别、导出照片、打包、上传、导入、核验和整理。原视频上传保持停用。明确的全局或单条暂停优先；网络、磁盘和文件占用恢复后继续。模型、阈值以及正常视频的抽样、区间规则保持原配置。

损坏位置最多尝试两次，每次最多 30 秒。只有工具、磁盘和文件版本正常，才把局部解码失败记为跳过；不会把后来画面当成原位置。有效照片按实际数量导入。未检出目标、未找到可用抽样画面和部分完成分别保留真实结论，不上传空包。

清理只涉及登记的下载视频、照片、ZIP 和临时文件。照片必须有对应版本、导入任务及唯一图片结果凭证；空成果必须有明确抽样结论。用户源目录、模型、未知文件和桌面原片保持不动。云端成果和网页历史保留。

## 持久接口

| 接口 | 用途 |
| --- | --- |
| `POST /api/collection-runs` | 仅登记日期查询，返回202；结果停在awaiting_review |
| `GET/PATCH /api/collection-runs/{id}/plan` | 分页查看计划、带修订号保存单条时间草稿 |
| `POST /api/collection-runs/{id}/submit` | 确认所选实绩，返回202与operationId |
| `GET /api/collection-operations/{id}` | 查询采集提交状态及逐条结果 |
| `POST /api/videos/{id}/withdraw` | 登记撤回，返回202与operationId；未确认的更正可恢复上一有效版 |
| `GET /api/collection-runs` | 分页读取采集摘要 |
| `GET /api/collection-runs/{id}` | 查询进度及分页质检明细 |
| `POST /api/videos/{id}/confirm-and-process` | 指定版本的最终确认，返回 202 与 operationId |
| `POST /api/videos/bulk-confirm-and-process` | 逐条登记批量确认，返回各条结果 |
| `GET /api/final-processing/{id}` | 查询真实阶段及完成原因 |
| `POST /api/videos/{id}/continue-processing` | 继续明确暂停的单条确认流程 |
| `GET /api/workspace/cleanup-summary` | 待清理数量与登记大小，无媒体扫描 |
| `POST /api/workspace/cleanup-completed` | 登记固定范围的手动清理 |
| `GET /api/workspace/cleanup-operations/{id}` | 查询逐条清理结果 |

写请求携带请求编号与当前修订。最终确认再绑定采集版本和处理代次；重放复用原操作，旧回调不会进入新版本。日期采集保存准入规则快照、分页游标、实绩身份和提交前凭证；提交结果不明时先对账。

兼容的 `/api/jobs` 提交需要显式 `confirmed: true`；缺少确认返回409。撤回状态由 `/api/video-operations/{id}` 查询；明确拒绝与响应丢失分开记录，结果不明时登记撤回意图等待对账，不凭同名认领任务。已最终确认的版本通过再次更正调整。

## 隔离验收

- 后端：`python -m pytest -q`，使用项目后端环境。
- 前端：`node --experimental-strip-types --test tests/*.test.mjs`、`npm run check`、`npm run build`。
- 页面：使用[界面验收](ui-validation.md)的隔离样例和当前操作清单；真实视频操作使用[交互体验](interactive-rehearsal.md)。
- 真实视频故障演练：`scripts/check_decode_pipeline.py`，只读源片、使用独立副本和模拟平台，具体参数见 `--help`。

测试媒体、数据库、报告及运行缓存不纳入源码发布。生产升级须安全停止写入并备份数据库，保留原请求、文件断点、人工更正及暂停状态。
