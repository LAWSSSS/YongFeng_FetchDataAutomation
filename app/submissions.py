"""Manual submission log. Deleting a row removes only the record."""
import json
from uuid import uuid4

from .domain import now_iso


class SubmissionLog:
    def __init__(self, db):
        from .record_visibility import RecordVisibility
        self.db = db
        self.visibility = RecordVisibility(db, 'receipts')
        self.item_visibility = RecordVisibility(db, 'receipt_items')
        db.execute("CREATE TABLE IF NOT EXISTS simple_submissions (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS simple_submission_hidden_jobs (id TEXT PRIMARY KEY)")
        db.commit()

    def import_jobs(self, jobs, describe):
        """Backfill once at startup; hiding a receipt must survive the next restart."""
        known = {(row.get("detail") or {}).get("jobId") for row in self.list("collection")}
        hidden = {row[0] for row in self.db.execute("SELECT id FROM simple_submission_hidden_jobs")}
        for job in jobs:
            if job["id"] not in known | hidden and not job.get("deletedAt"):
                self.add("collection", "采集提交", describe(job), submitted_at=job.get("createdAt") or "")

    def add(self, kind, title, detail, request_id="", submitted_at=""):
        if request_id:
            for row in self.list(kind):
                if row.get("requestId") == request_id:
                    return row
        row = {"id": uuid4().hex, "kind": kind, "title": title, "submittedAt": submitted_at or now_iso(),
               "requestId": request_id, "detail": detail}
        self.db.execute("INSERT INTO simple_submissions VALUES (?, ?)", (row["id"], json.dumps(row, ensure_ascii=False)))
        self.db.commit()
        return row

    def get(self, identity):
        row = self.db.execute("SELECT payload FROM simple_submissions WHERE id=?", (identity,)).fetchone()
        return json.loads(row[0]) if row else None

    def list(self, kind=""):
        rows = [json.loads(item[0]) for item in self.db.execute("SELECT payload FROM simple_submissions")]
        rows = [row for row in rows if not kind or row.get("kind") == kind]
        return sorted(rows, key=lambda row: row.get("submittedAt") or "", reverse=True)

    def delete(self, identity):
        row = self.get(identity)
        job_id = (row.get("detail") or {}).get("jobId") if row else None
        if job_id:
            self.db.execute("INSERT OR IGNORE INTO simple_submission_hidden_jobs VALUES(?)", (job_id,))
        self.db.execute("DELETE FROM simple_submissions WHERE id=?", (identity,))
        self.db.commit()
