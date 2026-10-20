"""Manual submission log. Deleting a row removes only the record."""
import json
from uuid import uuid4

from .domain import now_iso


class SubmissionLog:
    def __init__(self, db):
        self.db = db
        db.execute("CREATE TABLE IF NOT EXISTS simple_submissions (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        db.commit()

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
        self.db.execute("DELETE FROM simple_submissions WHERE id=?", (identity,))
        self.db.commit()
