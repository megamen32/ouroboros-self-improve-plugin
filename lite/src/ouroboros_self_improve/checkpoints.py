from __future__ import annotations
import json, pathlib
from datetime import datetime, timezone
from typing import Any, Dict, List

class CheckpointStore:
    def __init__(self, root: str | pathlib.Path):
        self.path = pathlib.Path(root) / "state" / "evolution_checkpoints.jsonl"

    def append(self, row: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = dict(row); data.setdefault("ts", datetime.now(timezone.utc).isoformat())
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

    def rows(self, max_entries: int = 200) -> List[Dict[str, Any]]:
        try: lines = self.path.read_text(encoding="utf-8").splitlines()[-max_entries:]
        except Exception: return []
        out=[]
        for line in lines:
            try:
                x=json.loads(line)
                if isinstance(x,dict): out.append(x)
            except Exception: pass
        return out

    def capability_digest(self) -> str:
        rows=self.rows(); counts={}; absorbed=[]; failed=[]
        for row in rows:
            outcome=str(row.get("cycle_outcome") or (row.get("transaction") or {}).get("cycle_outcome") or "unknown")
            counts[outcome]=counts.get(outcome,0)+1
            obj=str(row.get("campaign_objective") or "").replace("\n"," ").strip()
            if outcome=="absorbed" and obj: absorbed.append(obj)
            elif outcome in {"abandoned","no_op"} and obj: failed.append(obj)
        if not counts: return ""
        parts=["Cycle outcomes: "+", ".join(f"{k}={v}" for k,v in sorted(counts.items()))]
        parts += ["ABSORBED: "+x[:120] for x in absorbed[-8:]]
        parts += ["NOT LANDED: "+x[:120] for x in failed[-4:]]
        return "\n".join(parts)
