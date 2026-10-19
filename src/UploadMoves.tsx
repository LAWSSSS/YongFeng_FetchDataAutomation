import { useEffect, useRef, useState } from 'react';
import { auditBucket } from './upload-audit';

type MoveItem = {relativePath: string; source: string; destination?: string; category: string;
  state: string; message?: string; size: number};
type MovePlan = {id: string; reportId: string; sourceDirectory: string; uploadedDirectory: string;
  magnetMissingDirectory?: string; bedMissingDirectory?: string; ruleVersion?: number;
  status: string; message: string; requestId?: string;
  counts: Record<string,number>; items: MoveItem[]};

async function moveRequest<T>(path: string, body?: object): Promise<T> {
  let response: Response;
  try {response = await fetch(`/api/upload-audit/moves${path}`, body === undefined ? undefined : {
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),
  });} catch {throw new Error('本地程序暂时无法连接；移动进度会保留，连接后可刷新查看。');}
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || '无法处理移动请求，请刷新后重试。');
  return result as T;
}

export default function UploadMoves({report, auditRunning, onBusyChange, onDone}: {
  report: {id: string; directory: string; policyVersion?: number;
    files: {fileVersion?: object; issue?: string; targets: Record<string,{status: string}>}[]}; auditRunning: boolean;
  onBusyChange: (value: boolean) => void; onDone: () => Promise<void>;
}) {
  const [plan,setPlan] = useState<MovePlan | null>(null);
  const [uploaded,setUploaded] = useState('');
  const [magnetMissing,setMagnetMissing] = useState('');
  const [bedMissing,setBedMissing] = useState('');
  const [busy,setBusy] = useState(false);
  const [error,setError] = useState('');
  const [page,setPage] = useState(1);
  const confirming = useRef<{id: string; requestId: string} | null>(null);
  const edited = useRef(false);
  const notified = useRef('');
  const moving = plan?.status === 'running';
  const currentPlan = plan?.reportId === report.id;
  const currentRules = plan?.ruleVersion === 4;
  const currentAudit = report.policyVersion === 2;
  const versioned = report.files.every(file => !!file.fileVersion);
  useEffect(() => {
    let active = true;
    void moveRequest<{plan: MovePlan | null}>('').then(data => {
      if (!active) return;
      setPlan(data.plan);
      if (data.plan?.sourceDirectory === report.directory && !edited.current) {
        setUploaded(data.plan.uploadedDirectory);
        setMagnetMissing(data.plan.magnetMissingDirectory || `${report.directory}/磁盘吊未上传`);
        setBedMissing(data.plan.bedMissingDirectory || `${report.directory}/车斗未上传`);
      }
    }).catch(() => { /* This feature becomes available when the updated backend starts. */ });
    return () => {active = false;};
  }, []);
  useEffect(() => {
    const root = report.directory.replace(/\/+$/, '');
    setUploaded(old => old || `${root}/已上传`);
    setMagnetMissing(old => old || `${root}/磁盘吊未上传`);
    setBedMissing(old => old || `${root}/车斗未上传`);
  }, [report.directory]);
  useEffect(() => {onBusyChange(moving || busy);return () => onBusyChange(false);}, [moving,busy,onBusyChange]);
  useEffect(() => {
    if (!moving) return;
    let active = true;
    const timer = window.setInterval(() => void moveRequest<{plan: MovePlan | null}>('').then(data => {
      if (active) {setPlan(data.plan);setError('');}
    }).catch(failure => {if (active) setError((failure as Error).message);}),1500);
    return () => {active = false;window.clearInterval(timer);};
  }, [moving]);
  useEffect(() => {
    if (!plan || !currentPlan || ['preview','running'].includes(plan.status) || notified.current === `${plan.id}:${plan.status}`) return;
    notified.current = `${plan.id}:${plan.status}`;
    void onDone().catch(failure => setError((failure as Error).message));
  }, [plan?.id,plan?.status,currentPlan,onDone]);

  async function preview() {
    setBusy(true);setError('');
    try {
      setPlan(await moveRequest<MovePlan>('/preview', {reportId:report.id,uploadedDirectory:uploaded.trim(),
        magnetMissingDirectory:magnetMissing.trim(),bedMissingDirectory:bedMissing.trim(),refreshReport:true}));
      await onDone();
      confirming.current = null;setPage(1);
    } catch (failure) {setError((failure as Error).message);}
    finally {setBusy(false);}
  }
  async function confirm() {
    if (!plan) return;
    const requestId = plan.requestId || (confirming.current?.id === plan.id ? confirming.current.requestId : crypto.randomUUID());
    confirming.current = {id:plan.id,requestId};setBusy(true);setError('');
    try {setPlan(await moveRequest<MovePlan>(`/${plan.id}/confirm`,{requestId}));}
    catch (failure) {setError((failure as Error).message);}
    finally {setBusy(false);}
  }
  function edit(setter: (value: string) => void, value: string) {
    edited.current = true;
    setter(value);
    if (plan && !moving) setPlan(null);
  }
  const counts = plan?.counts || {};
  const pending = (counts.planned || 0) + (counts.moving || 0) + (counts.failed || 0);
  const shown = currentPlan ? plan.items.slice((page - 1) * 20, page * 20) : [];
  const pages = Math.max(1,Math.ceil((plan?.items.length || 0) / 20));
  const labels: Record<string,string> = {planned:'待移动',moving:'移动中',moved:'已剪切',unchanged:'已在正确目录',held:'保留原位',failed:'需要重试'};
  const categories = report.files.reduce<Record<string,number>>((result,file) => {
    const key = auditBucket(file); result[key] = (result[key] || 0) + 1; return result;
  }, {});
  return <section className="panel audit-moves"><div className="section-title"><h2>按上传状态移动视频</h2><span>两处均未上传的留在原处</span></div>
    <p className="move-explanation">两处都已上传的放入“已上传”；只缺磁盘吊的放入“磁盘吊未上传”，只缺车斗的放入“车斗未上传”。两处均未上传、待核对、导入中及同名冲突的文件留在原处。</p>
    <p className="move-summary">当前报告：两处已上传 {categories.uploaded || 0} · 只缺磁盘吊 {categories.missing_magnet || 0} · 只缺车斗 {categories.missing_bed || 0} · 两处均未上传 {categories.both_missing || 0} · 待核对 {categories.held || 0}</p>
    <div className="move-directories"><label>已上传视频放到<input aria-label="已上传视频目标文件夹" value={uploaded} disabled={moving || busy}
      onChange={e => edit(setUploaded,e.target.value)} /></label>
      <label>磁盘吊未上传的视频放到<input aria-label="磁盘吊未上传视频目标文件夹" value={magnetMissing} disabled={moving || busy}
        onChange={e => edit(setMagnetMissing,e.target.value)} /></label>
      <label>车斗未上传的视频放到<input aria-label="车斗未上传视频目标文件夹" value={bedMissing} disabled={moving || busy}
        onChange={e => edit(setBedMissing,e.target.value)} /></label></div>
    <p className="subtle move-explanation">原文件夹：{report.directory}。预览前会重新读取最新上传日志，并包含分类子文件夹。补传成功的视频会从“未上传”分类剪切到“已上传”，保留原有子目录结构；采集记录中的文件路径同步更新。</p>
    {plan && !currentPlan && <p className="notice">上传核对报告已更新，上次分类计划不会自动移动新完成的视频。请重新核对并预览分类，再确认剪切。</p>}
    {!versioned && <p className="notice">请先重新读取目录并核对，为文件记录版本后即可移动。</p>}
    {!currentAudit && <p className="notice">请先重新读取目录并核对，按本地文件创建时间更新分类结果。</p>}
    {currentPlan && !currentRules && <p className="notice">分类规则已更新，请重新预览。下方保留上次移动结果，已移动的文件位置保持不变。</p>}
    <div className="actions"><button className="secondary" disabled={busy || moving || auditRunning || !versioned || !currentAudit || !uploaded.trim() || !magnetMissing.trim() || !bedMissing.trim()}
      onClick={() => void preview()}>{busy ? '正在处理…' : '重新核对并预览分类'}</button>
      {currentPlan && currentRules && plan.status === 'preview' && <button disabled={busy || moving || auditRunning || !pending}
        onClick={() => void confirm()}>确认剪切 {pending} 个视频</button>}
      {currentPlan && currentRules && ['attention','interrupted'].includes(plan.status) && <button disabled={busy || auditRunning || !pending}
        onClick={() => void confirm()}>继续处理剩余 {pending} 个</button>}
    </div>
    {error && <p className="error" role="alert">{error}</p>}
    {currentPlan && currentRules && plan.status === 'preview' && !pending && <p className="notice">本次没有需要剪切的文件：已在正确目录 {counts.unchanged || 0} 个，保留原位 {counts.held || 0} 个。展开清单可查看每个原因。</p>}
    {currentPlan && <><p className="notice" role="status">{plan.message} · 已剪切 {counts.moved || 0} · 已在正确目录 {counts.unchanged || 0} · 保留原位 {counts.held || 0} · 待处理 {pending}
      {moving && <progress aria-label="视频分类移动进度" max={plan.items.length} value={(counts.moved || 0)+(counts.held || 0)+(counts.unchanged || 0)} />}</p>
      {currentRules && <p className="move-summary">{plan.status === 'preview' ? '本次拟移动' : '本次分类'}：已上传 {plan.items.filter(i => i.category === 'uploaded' && ['planned','moving','moved','failed'].includes(i.state)).length} 个 · 磁盘吊未上传 {plan.items.filter(i => i.category === 'missing_magnet' && ['planned','moving','moved','failed'].includes(i.state)).length} 个 · 车斗未上传 {plan.items.filter(i => i.category === 'missing_bed' && ['planned','moving','moved','failed'].includes(i.state)).length} 个 · 已在正确目录 {counts.unchanged || 0} 个 · 两处均未上传，留在原处 {plan.items.filter(i => i.category === 'both_missing').length} 个</p>}
      <details className="move-manifest"><summary>查看逐个文件的去向与原因（{plan.items.length}）</summary>
        <ul>{shown.map(item => <li key={item.relativePath}><div><strong>{item.relativePath}</strong><span>{labels[item.state] || item.state}</span></div>
          <p>{['held','unchanged'].includes(item.state) ? item.message : `移动到：${item.destination}`}</p>
          {item.state === 'failed' && <p className="error">{item.message}</p>}</li>)}</ul>
        {pages > 1 && <div className="audit-pagination"><button className="secondary" disabled={page <= 1} onClick={() => setPage(page - 1)}>上一页</button>
          <span>{page} / {pages}</span><button className="secondary" disabled={page >= pages} onClick={() => setPage(page + 1)}>下一页</button></div>}
      </details>
    </>}
  </section>;
}
