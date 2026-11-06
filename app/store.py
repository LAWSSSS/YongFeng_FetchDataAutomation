import json
import sqlite3
from pathlib import Path

from .domain import now_iso


class JobStore:
    def __init__(self, path: Path, managed_root: Path | None = None):
        path.parent.mkdir(parents=True, exist_ok=True)
        if managed_root is not None:
            from .managed_storage import ManagedConnection
            self.db = sqlite3.connect(path, factory=ManagedConnection)
            self.db.initialize(managed_root)
        else:
            self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, created_at TEXT, payload TEXT NOT NULL)")
        self.db.commit()
        for job in self.list():
            if job["status"] == "running":
                for item in job["items"]:
                    if item["status"] == "submitting":
                        item.update(status="uncertain", message="程序退出前正在提交，请先核对原站。")
                job["status"] = "attention"
                job["events"].append({"at": now_iso(), "message": "检测到上次运行中断；待核对的批次不会自动重发。"})
                self.save(job)

    def save(self, job: dict, *, commit=True):
        job["updatedAt"] = now_iso()
        self.db.execute(
            "INSERT INTO jobs VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            (job["id"], job["createdAt"], json.dumps(job, ensure_ascii=False)),
        )
        if commit:
            self.db.commit()

    def delete(self, job_id: str) -> None:
        self.db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        self.db.commit()

    def get(self, job_id: str) -> dict | None:
        row = self.db.execute("SELECT payload FROM jobs WHERE id=?", (job_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self) -> list[dict]:
        return [json.loads(row[0]) for row in self.db.execute("SELECT payload FROM jobs ORDER BY created_at DESC")]

    def known_keys(self, exclude_job: str = "", *, include_retired=True) -> set[str]:
        retired = ({row[0] for row in self.db.execute('SELECT key FROM retired_task_keys')}
                   if include_retired and self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retired_task_keys'").fetchone() else set())
        statuses = {"submitted", "uncertain", "submitting"}
        if include_retired:
            statuses.add("duplicate")
        return retired | {item["key"] for job in self.list() if job["id"] != exclude_job for item in job["items"]
                          if item["status"] in statuses or (item["status"] == "duplicate" and item.get("bindingVerified"))}

    def known_records(self, target):
        from .capture_history import same_target
        return {str(item["recordId"]) for job in self.list() if same_target(job.get("target"), target)
                for item in job["items"] if item.get("recordId")
                and (item["status"] in {"submitted", "pending", "uncertain", "submitting"}
                     or (item["status"] == "duplicate" and item.get("bindingVerified")))}

    def remove_history(self, job_id: str):
        job = self.get(job_id)
        if not job or job.get("deletedAt"):
            raise ValueError("记录不存在或已删除。")
        if job["status"] == "running":
            raise ValueError("请先停止该批次，再删除历史记录。")
        # 隐藏展示记录，提交凭证仍供去重、实绩关联和下载使用。
        job["deletedAt"] = now_iso()
        self.save(job)

    def close(self):
        self.db.close()
