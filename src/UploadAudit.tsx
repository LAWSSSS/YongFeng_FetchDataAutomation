import { useCallback, useEffect, useMemo, useState } from 'react';
import type { FormEvent } from 'react';
import './upload-audit.css';
import UploadQueue from './UploadQueue';
import { auditBucket } from './upload-audit';
import { usePolling } from './usePolling';

type FileStatus = 'uploaded' | 'missing' | 'in_progress' | 'review';
type Evidence = {status: FileStatus; message: string; jobId?: string; at?: string};
type AuditFile = {name: string; relativePath: string; size: number; targets: Record<string,Evidence>;
  createdAt?: string; issue?: string; fileVersion?: object; localMove?: {path: string; category: string; at: string}};
type Dataset = {id: string; name: string; url: string; counts?: Partial<Record<FileStatus,number>>;
  complete?: boolean; runningJobs?: number; importCount?: number; readImportCount?: number; skippedBeforeCreation?: number;
  issues?: {jobId: string; message: string}[]};
type AuditReport = {id: string; createdAt: string; directory: string; recursive: boolean; localCount: number;
  ignoredCount: number; datasets: Dataset[]; files: AuditFile[]; policyVersion?: number;
  scope?: {basis: string; since: string; unknownTimes: number}};
type AuditState = {workspaceManaged?:boolean;connected: boolean; user: string; directory: string; recursive?: boolean; datasets: Dataset[];
  savedConnection?: boolean; connectionWarning?: string;
  run: {id: string; status: string; message: string; totalLogs?: number; completedLogs?: number} | null;
  report: AuditReport | null};
const names: Record<FileStatus,string> = {uploaded:'已找到成功记录',missing:'未见上传记录',in_progress:'导入中',review:'待核对'};
const initial: AuditState = {connected:false,user:'',directory:'',datasets:[],run:null,report:null};

function beijing(value: string) {
  if (!/(?:Z|[+-]\d{2}:?\d{2})$/i.test(value)) return value.slice(0,19).replace('T',' ');
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('sv-SE', {timeZone:'Asia/Shanghai',hour12:false});
}

async function request(path: string, body?: object): Promise<AuditState> {
  let response: Response;
  try { response = await fetch(`/api/upload-audit${path}`, body === undefined ? undefined : {
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),
  }); } catch { throw new Error('本地程序暂时无法连接。'); }
  const data = await response.json();
  if (!response.ok) throw new Error(data.message || data.detail || '读取失败，请重新连接后重试。');
  return data;
}

function csvCell(value: string | number) {
  let text = String(value);
  if (/^[=+@\-\t\r]/.test(text)) text = `'${text}`;
  return `"${text.replaceAll('"','""')}"`;
}

export default function UploadAudit({defaultDirectory}: {defaultDirectory: string}) {
  const [state,setState] = useState<AuditState>(initial);
  const [directory,setDirectory] = useState('');
  const [recursive,setRecursive] = useState(false);
  const [account,setAccount] = useState('');
  const [password,setPassword] = useState('');
  const [busy,setBusy] = useState(false);
  const [moving,setMoving] = useState(false);
  const [error,setError] = useState('');
  const [notice,setNotice] = useState('');
  const [search,setSearch] = useState('');
  const [filter,setFilter] = useState('outstanding');
  const [page,setPage] = useState(1);
  const running = state.run?.status === 'running';
  const refreshReport = useCallback(async () => {setState(await request(''));}, []);
  useEffect(() => {
    let active = true;
    void request('').then(data => {
      if (!active) return;
      setState(data);setDirectory(value => value || data.directory);setRecursive(!!data.recursive);
    }).catch(failure => {if (active) setError((failure as Error).message);});
    return () => {active = false;};
  }, []);
  useEffect(() => {setDirectory(value => value || defaultDirectory);}, [defaultDirectory]);
  usePolling(async () => {await refreshReport();setError('');}, running ? 2000 : null,
    {immediate:false,onError:failure=>setError((failure as Error).message)});
  useEffect(() => {setPage(1);}, [search,filter,state.report?.id]);

  async function perform(path: string, body: object) {
    setBusy(true);setError('');setNotice('');
    try {const data = await request(path,body);setState(data);if (path === '/login') setPassword('');}
    catch (failure) {setError((failure as Error).message);}
    finally {setBusy(false);}
  }
  function login(event: FormEvent) {event.preventDefault();void perform('/login',{account,password});}
  function audit(event: FormEvent) {event.preventDefault();void perform('/run',{directory:state.directory,recursive:true});}
  const report = state.report;
  const rows = useMemo(() => (report?.files || []).filter(file => {
    if (search && !file.relativePath.toLocaleLowerCase().includes(search.toLocaleLowerCase())) return false;
    const first = file.targets['27912']?.status === 'uploaded';
    const second = file.targets['27944']?.status === 'uploaded';
    const bucket = auditBucket(file);
    if (filter === 'outstanding') return !first || !second;
    if (filter === 'both-missing') return bucket === 'both_missing';
    if (filter === 'missing-magnet') return bucket === 'missing_magnet';
    if (filter === 'missing-bed') return bucket === 'missing_bed';
    if (filter === '27912') return !first;
    if (filter === '27944') return !second;
    if (filter === 'uploaded') return first && second;
    if (filter === 'missing') return Object.values(file.targets).some(value => value.status === 'missing');
    if (filter === 'review') return Object.values(file.targets).some(value => ['review','in_progress'].includes(value.status));
    return true;
  }), [report,search,filter]);
  const pageCount = Math.max(1,Math.ceil(rows.length / 100));
  const currentPage = Math.min(page,pageCount);
  const visible = rows.slice((currentPage-1)*100,currentPage*100);

  function exportCsv() {
    if (!report) return;
    const header = ['文件名','相对路径','当前位置','字节数','本地文件创建时间',...report.datasets.flatMap(d => [`${d.name}状态`,`${d.name}任务ID`,`${d.name}说明`])];
    const values = rows.map(file => [file.name,file.relativePath,file.localMove?.path || `${report.directory}/${file.relativePath}`,file.size,file.createdAt ? beijing(file.createdAt) : '无法读取，按全量日志核对',...report.datasets.flatMap(d => {
      const value = file.targets[d.id];return [names[value.status],value.jobId || '',value.message];
    })]);
    const blob = new Blob(['\uFEFF'+[header,...values].map(row => row.map(csvCell).join(',')).join('\r\n')], {type:'text/csv;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');link.href=url;link.download=`上传核对_${beijing(report.createdAt).slice(0,10)}.csv`;link.click();URL.revokeObjectURL(url);
  }
  async function copyNames() {
    try {await navigator.clipboard.writeText(rows.map(file => file.relativePath).join('\n'));setNotice(`已复制当前筛选的 ${rows.length} 个文件名。`);}
    catch {setError('浏览器未允许复制，请使用“导出当前名单”。');}
  }

  return <div className="upload-audit">
    <section className="panel"><div className="section-title"><h2>视觉AI平台连接</h2><span className="audit-connection"><span className={state.connected ? 'dot connected' : 'dot'} />{state.connected ? `已连接 · ${state.user}${state.savedConnection ? ' · 已保存' : ''}` : state.savedConnection ? '连接已保存，等待恢复' : '尚未连接'}</span></div>
      {!state.connected ? <form className="audit-login" onSubmit={login}>
        <label>视觉AI平台账号<input autoComplete="username" value={account} onChange={e => setAccount(e.target.value)} required /></label>
        <label>密码<input type="password" autoComplete="current-password" value={password} onChange={e => setPassword(e.target.value)} required /></label>
        <button disabled={busy || running}>{busy ? '连接中…' : '连接视觉AI平台'}</button>
      </form> : <button className="link" disabled={busy || running} onClick={() => void perform('/logout',{})}>断开并清除保存连接</button>}
      <p className="subtle">连接 pre-athena-mv.cisdigital.cn；与采集平台及 Edge 的登录会话独立。登录后仅将平台刷新凭证保存在本机系统钥匙串，密码不会保存；自动上传仅在点击“扫描并补传”后添加视频。</p>
      {state.connectionWarning && <p className="error" role="status">{state.connectionWarning}</p>}
    </section>

    <UploadQueue onSessionChange={refreshReport} />

    <section className="panel"><div className="section-title"><h2>核对工作区视频</h2><span>两个数据集分别核对</span></div>
      <form onSubmit={audit}><p className="audit-path">{state.directory || '请先启用文件工作区'}</p><button disabled={!state.connected || busy || running || !state.directory}>{running ? '正在核对…' : '读取目录并核对'}</button></form>
      <p className="subtle">按每个视频的本地文件创建时间起核对，只读取相关的“数据集导入”日志，跳过更早的任务及视频切片。按完整名称匹配；本地创建时间无法读取时按全量日志核对。</p>
      {state.run && <div className={['failed','interrupted'].includes(state.run.status) ? 'notice audit-run failed' : 'notice audit-run'} role="status">
        {state.run.message}{running && !!state.run.totalLogs && <progress aria-label="日志核对进度" max={state.run.totalLogs} value={state.run.completedLogs || 0} />}
      </div>}
      {error && <p className="error" role="alert">{error}</p>}{notice && <p className="success" role="status">{notice}</p>}
    </section>

    {report ? <>
    <div className="audit-summary">{report.datasets.map(dataset => <section className="panel" key={dataset.id}>
      <div className="section-title"><h2>{dataset.name}</h2><a href={dataset.url} target="_blank" rel="noreferrer">详情与日志 ↗</a></div>
      <p className="audit-counts"><strong>{dataset.counts?.uploaded || 0}</strong> 已找到成功记录 <span>·</span> <strong>{dataset.counts?.missing || 0}</strong> 未见上传记录 <span>·</span> <strong>{(dataset.counts?.review || 0)+(dataset.counts?.in_progress || 0)}</strong> 待核对或导入中</p>
      <p className="subtle">本次读取 {dataset.readImportCount ?? dataset.importCount} 条导入日志，跳过创建时间之前的任务 {dataset.skippedBeforeCreation || 0} 条；运行中 {dataset.runningJobs || 0} 条。{dataset.complete ? '相关日志已读完。' : '部分日志缺失，仅影响创建时间落在相关范围内的视频。'}</p>
      {!!dataset.issues?.length && <details className="audit-issues"><summary>{dataset.issues.length} 条日志需要核对</summary><ul>{dataset.issues.map((issue,index) => <li key={`${issue.jobId}-${index}`}>任务 {issue.jobId}：{issue.message}</li>)}</ul></details>}
    </section>)}</div>
    <section className="panel"><div className="section-title"><h2>上传核对结果</h2><span>{running || state.run?.id !== report.id ? '上次报告 · ' : ''}{beijing(report.createdAt)} · 北京时间</span></div>
      <p className="subtle audit-path">目录：{report.directory} · 本地视频 {report.localCount} 个 · 忽略其他文件 {report.ignoredCount} 个{report.recursive ? ' · 含子目录' : ''}</p>
      {report.scope && <p className="subtle">每个文件分别从本地创建时间起核对。{report.scope.since ? `本次最早起点：${beijing(report.scope.since)}（北京时间）。` : ''}
        {!!report.scope.unknownTimes && `${report.scope.unknownTimes} 个文件无法读取创建时间，已纳入全量日志核对。`}</p>}
      <div className="audit-tools"><label>文件名筛选<input aria-label="核对文件名筛选" value={search} onChange={e => setSearch(e.target.value)} placeholder="车牌、时间或完整文件名" /></label>
        <label>显示范围<select value={filter} onChange={e => setFilter(e.target.value)}>
          <option value="outstanding">尚未全部确认上传</option><option value="missing-magnet">只缺磁盘吊，可分类移动</option><option value="missing-bed">只缺车斗，可分类移动</option>
          <option value="both-missing">两处均未上传，留在原处</option><option value="missing">日志完整且未见上传记录</option>
          <option value="27912">车斗检测未找到成功记录</option><option value="27944">磁盘吊未找到成功记录</option>
          <option value="review">待核对或导入中</option><option value="uploaded">两处都有成功记录</option><option value="all">全部本地视频</option>
        </select></label><button className="secondary" disabled={!rows.length} onClick={exportCsv}>导出当前名单</button>
        <button className="secondary" disabled={!rows.length} onClick={() => void copyNames()}>复制文件名</button>
      </div>
      <p className="record-counts">当前筛选 {rows.length} / {report.localCount} 个文件。运行中、失败或缺少完整日志的项目会标为待核对。</p>
      {visible.length ? <div className="audit-table-scroll"><table><thead><tr><th>本地视频</th>{report.datasets.map(d => <th key={d.id}>{d.name}</th>)}</tr></thead><tbody>{visible.map(file => <tr key={file.relativePath}>
        <td><strong className="audit-filename">{file.relativePath}</strong><small className="subtle">{(file.size/1024**3).toFixed(2)} GB</small>
          <small className="subtle">本地创建：{file.createdAt ? beijing(file.createdAt) : report.policyVersion === 2 ? '无法读取；已核对全量日志' : '未记录；需重新核对'}</small>{file.localMove && <p className="move-location">已移动到：{file.localMove.path}</p>}</td>
        {report.datasets.map(d => {const value=file.targets[d.id];return <td key={d.id}><span className={`audit-status ${value.status}`}>{names[value.status]}</span><p>{value.message}</p>
          {value.jobId && <small className="subtle">任务 {value.jobId}{value.at ? ` · ${beijing(value.at)}` : ''}</small>}</td>;})}
      </tr>)}</tbody></table></div> : <p className="empty">{filter === 'outstanding' ? '当前没有尚待确认上传的本地视频。' : '没有符合当前筛选的文件。'}</p>}
      {pageCount > 1 && <div className="audit-pagination"><button className="secondary" disabled={currentPage===1} onClick={() => setPage(currentPage-1)}>上一页</button><span>{currentPage} / {pageCount} 页</span><button className="secondary" disabled={currentPage===pageCount} onClick={() => setPage(currentPage+1)}>下一页</button></div>}
    </section></> : <section className="panel audit-empty"><h2>把本地文件与两份日志逐一对上</h2><p>连接视觉AI平台后开始核对。成功任务中的完整文件名会作为依据；日志读取失败时会保留上一份报告。</p><div className="audit-targets">{state.datasets.map(d => <a href={d.url} key={d.id} target="_blank" rel="noreferrer">{d.name} · {d.id} ↗</a>)}</div></section>}
  </div>;
}
