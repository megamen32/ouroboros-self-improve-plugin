from __future__ import annotations
import hashlib, json, os, pathlib, tempfile
from typing import Any, Dict, List

class BacklogStore:
    """Small structured substitute for Ouroboros' markdown improvement backlog.

    It preserves the important extraction semantics: durable append, stable exact
    fingerprints, recurrence counting, open/done state, priority and plan-review flag.
    """
    def __init__(self, root: str | pathlib.Path):
        self.path = pathlib.Path(root) / "state" / "improvement_backlog.json"

    @staticmethod
    def fingerprint(item: Dict[str, Any]) -> str:
        key = " | ".join(" ".join(str(item.get(k) or "").split()).lower() for k in ("summary", "category", "source"))
        return hashlib.sha256(key.encode()).hexdigest()[:12]

    def load(self) -> List[Dict[str, Any]]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _write(self, items: List[Dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=self.path.name + ".", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
                f.write("\n")
            os.replace(tmp, self.path)
        finally:
            try: os.unlink(tmp)
            except FileNotFoundError: pass

    def append(self, candidates: List[Dict[str, Any]]) -> int:
        items = self.load()
        by_fp = {str(x.get("fingerprint") or ""): x for x in items if x.get("fingerprint")}
        added = 0
        for raw in candidates:
            if not str(raw.get("summary") or "").strip() or not str(raw.get("evidence") or "").strip():
                continue
            x = dict(raw)
            fp = self.fingerprint(x)
            if fp in by_fp and str(by_fp[fp].get("status") or "open") != "done":
                by_fp[fp]["count"] = int(by_fp[fp].get("count") or 1) + 1
                continue
            x.setdefault("id", f"ibl-{fp}")
            x["fingerprint"] = fp
            x.setdefault("status", "open")
            x.setdefault("priority", "med")
            x.setdefault("kind", "improvement")
            x.setdefault("requires_plan_review", True)
            x.setdefault("count", 1)
            items.append(x); by_fp[fp] = x; added += 1
        if added or candidates:
            self._write(items)
        return added

    def close(self, backlog_id: str) -> bool:
        items = self.load(); changed = False
        for x in items:
            if str(x.get("id")) == backlog_id and str(x.get("status") or "open") != "done":
                x["status"] = "done"; changed = True
        if changed: self._write(items)
        return changed

    def digest(self, limit: int = 8) -> str:
        open_items = [x for x in self.load() if str(x.get("status") or "open") != "done"]
        open_items.sort(key=lambda x: ({"high":0,"med":1,"low":2}.get(str(x.get("priority") or "med"),1), -int(x.get("count") or 1)))
        return "\n".join(f"- {x.get('id')}: [{x.get('priority','med')}] {x.get('summary','')} (count={x.get('count',1)})" for x in open_items[:limit])
