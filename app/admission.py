"""Saved admission preferences; capture jobs keep independent rule snapshots."""
import json

from .models import AdmissionRule


def admission_rule(record=None, rule=None):
    value = rule if rule is not None else (record or {}).get("admissionRule")
    return value if isinstance(value, AdmissionRule) else AdmissionRule.model_validate(value or {})


def admission_summary(rule):
    rule = admission_rule(rule=rule)
    identity = []
    if rule.requireRealPlate:
        identity.append("有真实车牌")
    if rule.requireIntelligentPhotos:
        identity.append(f"智能判级有效照片至少 {rule.minPhotoCount} 张")
    joined = (" 或 " if rule.matchMode == "any" else " 且 ").join(identity)
    duration = f"质检时长严格超过 {rule.minInspectionMinutes:g} 分钟"
    return f"（{joined}），且{duration}" if len(identity) > 1 else f"{joined}，且{duration}" if joined else f"不要求车牌和照片；{duration}"


class AdmissionSettings:
    def __init__(self, db):
        self.db = db
        db.execute("CREATE TABLE IF NOT EXISTS simple_admission_settings "
                   "(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, payload TEXT NOT NULL)")
        db.commit()

    def state(self):
        row = self.db.execute("SELECT revision,payload FROM simple_admission_settings WHERE id=1").fetchone()
        rule = AdmissionRule.model_validate(json.loads(row[1])) if row else AdmissionRule()
        return {"revision": row[0] if row else 0, "rule": rule.model_dump(), "summary": admission_summary(rule)}

    def save(self, rule, expected_revision):
        current = self.state()
        if current["revision"] != expected_revision:
            # Imported lazily to keep domain validation independent of video managers.
            from .videos import Conflict
            raise Conflict("准入条件已在其他页面修改，请重新读取已保存条件后再保存。")
        value = admission_rule(rule=rule).model_dump()
        exists = self.db.execute("SELECT 1 FROM simple_admission_settings WHERE id=1").fetchone()
        if value != current["rule"] or not exists:
            self.db.execute("INSERT INTO simple_admission_settings VALUES(1,?,?) "
                            "ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload",
                            (current["revision"] + 1, json.dumps(value, ensure_ascii=False)))
            self.db.commit()
        return self.state()
