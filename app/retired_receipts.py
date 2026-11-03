"""Read-only reconciliation of previously submitted, explicitly retired jobs."""
import asyncio
import json
from .domain import now_iso


class RetiredReceipts:
    def __init__(self, db, photo_remote, slicing_remote, videos=None):
        self.db, self.photo_remote, self.slicing_remote = db, photo_remote, slicing_remote
        db.execute('CREATE TABLE IF NOT EXISTS retired_pending (id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        db.commit()
        self.task = None
        self.videos = videos

    async def tick(self):
        rows = [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM retired_pending')]
        inventories = {}
        for row in rows:
            if row.get('state') != 'waiting' or not row.get('taskId'):
                continue
            if row['kind'] == 'capture':
                if self.videos is None:
                    continue
                if 'captures' not in inventories:
                    inventories['captures'] = await self.videos.jobs.remote_parents(self.videos.target_config)
                remote = inventories['captures'].get(row['taskId'])
                if not remote or not self.videos.matches(remote, row['version'], self.videos.target_config['cameraId']):
                    continue
            elif row['kind'] == 'photo-import':
                dataset = row['dataset']
                if dataset not in inventories:
                    inventories[dataset] = await self.photo_remote.jobs(dataset)
                remote = next((item for item in inventories[dataset] if str(item.get('id')) == row['taskId']), None)
            else:
                remote = await self.slicing_remote.job(row['taskId'])
            if not remote:
                continue
            status = str(remote.get('taskStatus') or remote.get('status', ''))
            if status not in {'3','4','5','FIXED','ENDED','FAIL','ABNORMAL_END'}:
                continue
            # This is only a retired platform receipt, not image verification.
            row.update(state='reconciled', platformStatus=status, checkedAt=now_iso())
            self.db.execute('UPDATE retired_pending SET payload=? WHERE id=?', (json.dumps(row),row['id']))
            self.db.commit()

    async def start(self):
        self.task = asyncio.create_task(self.loop())

    async def loop(self):
        while True:
            try:
                await self.tick()
            except Exception:
                pass  # No retries can submit, delete, download or change a current video.
            await asyncio.sleep(30)

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
