import { useEffect, useRef, useState } from 'react';
import ProcessLimitControl from './ProcessLimitControl';

export type DownloadWorkers = {active: number; queued: number; max: number; revision: number};
export type DownloadLimitInput = {maxConcurrentDownloads: number; expectedRevision: number; requestId: string};

export default function DownloadManager({workers, onSave, onRefresh}: {
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
    <div className="section-title"><h2>进程管理</h2><span>下载与切片独立运行</span></div>
    <section className="slice-management-card">
      <h3>视频下载</h3>
      <p className="slice-management-summary" role="status">{workers
        ? `正在运行 ${workers.active} 条 · 排队 ${workers.queued} 条 · 当前上限 ${workers.max} 条`
        : '正在读取下载队列…'}</p>
      <ProcessLimitControl label="同时下载视频数（1–8）" value={draft} dirty={dirty} disabled={!workers}
        saving={saving} buttonLabel="保存下载数量" onSave={() => void save()}
        onChange={value => {setDraft(value);setDirty(true);pending.current = null;setError('');setNotice('');}} />
      <p className="subtle">默认 5 条，保存后重启仍生效。调低后，正在下载的视频继续，新任务按新上限启动。校验与保存计入运行数量。</p>
      {error && <p className="error" role="alert">{error}</p>}{notice && <p className="success" role="status">{notice}</p>}
    </section>
  </section>;
}
