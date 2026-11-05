import {useCallback, useEffect, useRef, useState} from 'react';
import {FolderOpen} from '@phosphor-icons/react';
import {usePolling} from './usePolling';
import {createReadCycle, localJson} from './local-api';
import {requestIntents} from './request-intent';
import CompletedCleanup from './CompletedCleanup';
import './workspace.css';

type Migration = {
  id: string; root: string; state: string; message: string; totalItems: number; bytes: number;
  counts: Record<string, number>; preserved?: number; canCancel?: boolean;
};
type State = {
  revision: number; root: string; defaultRoot?: string; online: boolean; maintenance: boolean;
  totalBytes: number; freeBytes?: number; availability?: {message?: string}; migration?: Migration;
};
type Action = 'preview' | 'default' | 'apply' | 'cancel' | 'retry' | 'open';

const size = (bytes: number) => bytes >= 1024 ** 3 ? `${(bytes / 1024 ** 3).toFixed(1)} GB` :
  bytes ? `${(bytes / 1024 ** 2).toFixed(1)} MB` : '0 MB';

export default function Workspace({onChanged}: {onChanged: () => Promise<void>}) {
  const [state, setState] = useState<State | null>(null);
  const [path, setPath] = useState('');
  const [editing, setEditing] = useState(false);
  const [busy, setBusy] = useState<Action | null>(null);
  const [error, setError] = useState('');
  const [readError, setReadError] = useState('');
  const reads = useRef(createReadCycle());
  const intents = useRef(requestIntents());
  const locked = useRef(false);
  const previousRevision = useRef<number | undefined>(undefined);
  const refresh = useCallback(async () => {
    if (locked.current) return;
    await reads.current.run('storage', async context => {
      try {
        const value = await localJson<State>('/api/workspace', undefined, {signal: context.signal});
        if (context.current()) {
          setState(value);
          setReadError('');
        }
      } catch (failure) {
        if (context.current()) setReadError((failure as Error).message);
      }
    });
  }, []);
  useEffect(() => { void refresh(); return () => reads.current.cancel(); }, [refresh]);
  useEffect(() => {
    if (!state) return;
    const previous = previousRevision.current;
    previousRevision.current = state.revision;
    if (previous !== undefined && previous !== state.revision) void onChanged();
  }, [state, onChanged]);
  usePolling(refresh, state?.maintenance ? 2000 : state?.online ? 30000 : 5000, {immediate: false});

  async function perform(action: Action, task: () => Promise<void>) {
    if (locked.current) return;
    locked.current = true;
    setBusy(action);
    setError('');
    reads.current.cancel();
    try {
      await task();
      intents.current.complete(action);
    } catch (failure) {
      setError((failure as Error).message);
    } finally {
      locked.current = false;
      setBusy(null);
      void refresh();
    }
  }

  function preview(useDefault = false) {
    if (!state?.online || state.maintenance) return;
    const parent = path.trim();
    if (!useDefault && !parent) return;
    const action = useDefault ? 'default' : 'preview';
    return perform(action, async () => {
      const body = {expectedRevision: state.revision, ...(useDefault ? {useDefault: true} : {parent})};
      const migration = await localJson<Migration>('/api/workspace/preview', intents.current.payload(action, body));
      setState({...state, migration});
      setEditing(false);
    });
  }

  const migration = state?.migration;
  function update(action: 'apply' | 'cancel' | 'retry') {
    if (!state || !migration) return;
    return perform(action, async () => {
      const result = await localJson<State>(`/api/workspace/${migration.id}/${action}`,
        intents.current.payload(action, {expectedRevision: state.revision}));
      setState(result);
      setEditing(false);
      if (action !== 'cancel') setPath('');
    });
  }

  const canChange = state?.online && !state.maintenance && !busy;
  const isPreview = migration?.state === 'preview';
  const migrationActive = migration && ['waiting', 'moving'].includes(migration.state);
  const done = migration?.counts.complete || 0;
  const held = migration?.counts.attention || 0;

  return <section className="panel workspace-panel" aria-label="本地数据">
    <div className="section-title"><div>
      <h2><FolderOpen size={22} aria-hidden="true"/>本地数据</h2>
      <p>下载视频、切片照片、压缩包和媒体缓存共用一个工作区。</p>
    </div><div className="workspace-actions">
      <button className="secondary" disabled={!state?.online || !!busy}
        onClick={() => void perform('open', async () => { await localJson('/api/workspace/open', {}); })}>
        {busy === 'open' ? '正在打开…' : '在访达打开'}
      </button>
      {!isPreview && <button className="secondary" disabled={!canChange} aria-expanded={editing}
        aria-controls="workspace-location" onClick={() => { setEditing(!editing); setError(''); }}>
        {editing ? '收起位置设置' : '更改保存位置'}
      </button>}
    </div></div>
    {state && <>
      <p className="workspace-current-path"><span>当前工作区</span><code>{state.root || '尚未设置'}</code></p>
      <p className="storage-summary" role="status"><span className={`status-dot ${state.online ? 'online' : 'offline'}`}/>
        {state.online ? '可用' : '等待磁盘恢复'} · 已登记 {size(state.totalBytes)}
        {state.online && typeof state.freeBytes === 'number' && ` · 可用空间 ${size(state.freeBytes)}`}
      </p>
    </>}
    {editing && <div id="workspace-location" className="workspace-location-settings">
      <form className="workspace-location" onSubmit={event => { event.preventDefault(); void preview(); }}>
        <label htmlFor="workspace-parent">保存到文件夹
          <input id="workspace-parent" value={path} onChange={event => setPath(event.target.value)}
            aria-describedby="workspace-location-hint" placeholder="/Volumes/工作盘" autoComplete="off"
            spellCheck={false} maxLength={4000} disabled={!canChange}/>
        </label>
        <button disabled={!canChange || !path.trim()}>{busy === 'preview' ? '正在核对…' : '预览迁移'}</button>
        {state?.defaultRoot && state.root !== state.defaultRoot && <button type="button" className="secondary"
          disabled={!canChange} onClick={() => void preview(true)}>
          {busy === 'default' ? '正在核对…' : '恢复默认位置'}
        </button>}
      </form>
      <p id="workspace-location-hint" className="subtle">
        可在访达选中文件夹，按 Option–Command–C 复制路径。程序会在所选位置建立“视频工作区”，任务记录仍保存在程序旁。
      </p>
    </div>}
    {isPreview && <div className="workspace-preview" aria-label="迁移预览">
      <p><strong>保存到</strong> <code>{migration.root}</code></p>
      <p className="subtle">预计迁移 {migration.totalItems} 个登记文件，约 {size(migration.bytes)}。
        {held > 0 && `其中 ${held} 个需核对。`}
        {migration.preserved ? `已有 ${migration.preserved} 条指定保留原位。` : ''}
        未登记的文件保留原位。</p>
      <div className="workspace-actions">
        <button disabled={!canChange} onClick={() => void update('apply')}>
          {busy === 'apply' ? '正在登记…' : migration.totalItems ? '迁移并使用' : '使用此位置'}
        </button>
        <button className="secondary" disabled={!!busy} onClick={() => void update('cancel')}>取消</button>
      </div>
    </div>}
    {migrationActive && <div className="workspace-migration" role="status">
      <strong>正在更改保存位置</strong>
      <p>{migration.message}</p>
      {migration.totalItems > 0 && <p className="subtle">已迁移 {done} / {migration.totalItems} 个文件
        {held > 0 && ` · ${held} 个需核对`}</p>}
      {migration.canCancel && <button className="secondary" disabled={!!busy}
        onClick={() => void update('cancel')}>取消迁移</button>}
    </div>}
    {migration?.state === 'complete' && <p className="notice" role="status">{migration.message}</p>}
    {migration?.state === 'attention' && <div className="workspace-migration">
      <p className="notice" role="status">{migration.message}{held > 0 && ` ${held} 个文件待核对。`}</p>
      {migration.root === state?.root && held > 0 && <button className="secondary" disabled={!canChange}
        onClick={() => void update('retry')}>重试迁移</button>}
    </div>}
    {!state?.online && state?.availability?.message && <p className="notice">{state.availability.message}</p>}
    {error && <p className="error" role="alert">{error}</p>}
    {readError && <p className="error" role="alert">{readError} <button className="secondary" type="button"
      onClick={() => void refresh()}>刷新状态</button></p>}
    <CompletedCleanup embedded blocked={state?.maintenance} key={state?.revision}/>
  </section>;
}
