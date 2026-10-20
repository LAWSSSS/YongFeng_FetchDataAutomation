import json
import sqlite3
from pathlib import Path

from .domain import now_iso


class JobStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
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

    def save(self, job: dict):
        job["updatedAt"] = now_iso()
        self.db.execute(
            "INSERT INTO jobs VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            (job["id"], job["createdAt"], json.dumps(job, ensure_ascii=False)),
        )
        self.db.commit()

    def delete(self, job_id: str) -> None:
        self.db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        self.db.commit()

    def get(self, job_id: str) -> dict | None:
        row = self.db.execute("SELECT payload FROM jobs WHERE id=?", (job_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self) -> list[dict]:
        return [json.loads(row[0]) for row in self.db.execute("SELECT payload FROM jobs ORDER BY created_at DESC")]

    def known_keys(self, exclude_job: str = "") -> set[str]:
        return {item["key"] for job in self.list() if job["id"] != exclude_job for item in job["items"]
                if item["status"] in ("submitted", "duplicate", "uncertain", "submitting")}

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
