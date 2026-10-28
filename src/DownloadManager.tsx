import { useEffect, useRef, useState } from 'react';
import ProcessLimitControl from './ProcessLimitControl';

export type DownloadWorkers = {active: number; queued: number; max: number; revision: number};
export type DownloadLimitInput = {maxConcurrentDownloads: number; expectedRevision: number; requestId: string};

export default function DownloadManager({workers, directory, checkError, onSave, onRefresh}: {
  directory?:string;checkError?:string;
  workers?: DownloadWorkers; onSave: (payload: DownloadLimitInput) => Promise<DownloadWorkers>;
  onRefresh: () => Promise<void>;
}) {
  const [draft, setDraft] = useState(workers?.max ?? 5);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const pending = useRef<DownloadLimitInput | null>(null);
  useEffect(() => {if (!dirty && workers) setDraft(workers.max);}, [dirty, workers?.max]);

  async function save() {
    if (!workers || saving || !Number.isInteger(draft) || draft < 1 || draft > 8) return;
    setSaving(true);setError('');setNotice('');
    pending.current ??= {maxConcurrentDownloads:draft, expectedRevision:workers.revision, requestId:crypto.randomUUID()};
    try {
      const result = await onSave(pending.current);
      setDraft(result.max);setDirty(false);pending.current = null;
      setNotice(`同时下载上限已设为 ${result.max} 条。`);
    } catch (failure) {
      setError((failure as Error).message);
      await onRefresh().catch(() => undefined);
      // A new click can use a refreshed revision after a conflict. A lost
      // response remains safe: saving a limit never resubmits a video task.
      pending.current = null;
    } finally {setSaving(false);}
  }

  return <section className="panel slicing-management download-management" aria-label="下载进程管理">
    <div className="section-title"><h2>下载进程管理</h2><span>与切片独立运行</span></div>
      <p className="slice-management-summary" role="status">{workers
        ? `正在运行 ${workers.active} 条 · 排队 ${workers.queued} 条 · 当前上限 ${workers.max} 条`
        : '正在读取下载队列…'}</p>
      <ProcessLimitControl label="同时下载视频数（1–8）" value={draft} dirty={dirty} disabled={!workers}
        saving={saving} buttonLabel="保存下载数量" onSave={() => void save()}
        onChange={value => {setDraft(value);setDirty(true);pending.current = null;setError('');setNotice('');}} />
      <details className="operation-details"><summary>保存位置与说明</summary><p className="subtle">{directory||'请先启用文件工作区。'}<br/>下载数量包含校验与保存；调整后生效，现有下载继续。断盘保留断点。</p></details>
      {checkError&&<p className="notice">{checkError}</p>}
      {error && <p className="error" role="alert">{error}</p>}{notice && <p className="success" role="status">{notice}</p>}
  </section>;
}
