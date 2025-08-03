from typing import Dict, Any
import copy

def deep_merge(original: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(original)
    for k, v in new.items():
        if isinstance(v, dict) and k in result:
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result