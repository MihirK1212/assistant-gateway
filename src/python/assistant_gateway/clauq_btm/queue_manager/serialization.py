from __future__ import annotations

import json
from typing import Any, Dict

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask


def serialize_for_redis_hset(data: Dict[str, Any]) -> Dict[str, str]:
    return {
        k: (
            json.dumps(v)
            if isinstance(v, (dict, list))
            else str(v) if v is not None else ""
        )
        for k, v in data.items()
    }


def serialize_task(task: ClauqBTMTask) -> Dict[str, Any]:
    return task.model_dump(mode="json", exclude={"executor"})


def deserialize_task(data: Dict[str, Any]) -> ClauqBTMTask:
    return ClauqBTMTask.model_validate(data)
