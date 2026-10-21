import { useCallback, useRef, useState } from 'react';
import { batchDate } from './record-groups';
import { usePolling } from './usePolling';

type Item = {videoName?: string; startTime?: string; endTime?: string; status?: string; camera?: string};
type RecordRow = {
  id: string; title: string; submittedAt: string; status?: string; submitted?: number; total?: number;
  detail?: {message?: string; phase?: string; requestId?: string; jobId?: string; items?: Item[]};
};

const statusText: Record<string, string> = {running: '提交中', complete: '提交结束', attention: '待核对', stopped: '已停止'};

async function load(kind: string) {
  const response = await fetch(`/api/submissions?kind=${kind}`);
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || result.detail || '记录暂时无法读取。');
  return result as RecordRow[];
}

export default function SubmissionRecords({kind, onChanged}: {kind: 'collection' | 'slicing'; onChanged?: () => void}) {
  const [rows, setRows] = useState<RecordRow[]>([]);
  const [error, setError] = useState('');
  const [open, setOpen] = useState('');
  const [working, setWorking] = useState('');
  const sequence = useRef(0);
  const refresh = useCallback(async () => {
    const current = ++sequence.current;
    try {const result = await load(kind); if (current === sequence.current) {setRows(result); setError('');}}
    catch (failure) {setError((failure as Error).message);}
  }, [kind]);
  usePolling(refresh, 8000);
  async function action(identity: string, task: () => Promise<void>) {
    if (working) return;
    ++sequence.current; setWorking(identity); setError('');
    try {await task(); await refresh(); onChanged?.();}
    catch (failure) {setError((failure as Error).message || '操作未完成，请重试。');}
    finally {setWorking('');}
  }
  async function remove(row: RecordRow) {
    if (!window.confirm('只删除这条提交记录，不删除已保存的视频。')) return;
    await action(row.id, async () => {
    const response = await fetch(`/api/submissions/${row.id}`, {method: 'DELETE'});
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(result.message || result.detail || '记录暂时无法删除。');
    if (open === row.id) setOpen('');
    });
  }
  async function reconcile(jobId: string) {
    await action(jobId, async () => {
    const response = await fetch(`/api/jobs/${jobId}/refresh`, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'});
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(result.message || result.detail || '暂时无法核对。');
    });
  }
  const title = kind === 'collection' ? '采集提交记录' : '切片提交记录';
  return <section className="panel"><div className="section-title"><h2>{title}</h2><span>时间是每次点击提交的时刻</span></div>
    {error && <p className="error" role="alert">{error}</p>}
    {rows.length === 0 && <p className="subtle">还没有人工提交记录。</p>}
    <div className="job-list">{rows.map(row => {
      const items = row.detail?.items || [];
      return <div key={row.id}>
        <button className="link" aria-expanded={open === row.id} onClick={() => setOpen(current => current === row.id ? '' : row.id)}>{batchDate(row.submittedAt)}</button>
        <span>{row.title}{row.status ? ` · ${statusText[row.status] || row.status}` : ''}{row.total != null ? ` · 已提交 ${row.submitted || 0} / ${row.total} 条` : ''}</span>
        {row.detail?.jobId && <button className="link" disabled={row.status === 'running' || !!working} onClick={() => void reconcile(row.detail?.jobId || '')}>{working === row.detail.jobId ? '核对中…' : '核对平台'}</button>}
        <button className="link" disabled={row.status === 'running' || !!working} onClick={() => void remove(row)}>{working === row.id ? '删除中…' : '删除记录'}</button>
        {open === row.id && <div className="table-scroll">{items.length > 0 ? <table><thead><tr><th>视频</th><th>开始</th><th>结束</th><th>状态</th></tr></thead><tbody>
          {items.map((item, index) => <tr key={`${item.videoName}-${item.startTime}-${index}`}><td>{item.videoName || '未命名'}</td><td>{item.startTime || '—'}</td><td>{item.endTime || '—'}</td><td>{item.status || '—'}</td></tr>)}
        </tbody></table> : <p className="subtle">{row.detail?.message || '已记录这次提交。'}{row.detail?.phase ? ` · ${row.detail.phase}` : ''}</p>}</div>}
      </div>;
    })}</div>
  </section>;
}
