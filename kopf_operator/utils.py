import os
import yaml
import copy
from typing import Dict, Any
from jinja2 import Template

DEFAULTS_DIR = os.getenv("DEFAULTS_DIR", os.path.join(os.path.dirname(__file__), "../defaults"))

def load_defaults(kind: str) -> Dict[str, Any]:
    """
    Load default spec for a given kind from YAML file located in DEFAULTS_DIR.
    """
    path = os.path.join(DEFAULTS_DIR, f"{kind.lower()}.yaml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Default spec file not found: {path}")
    
    with open(path, "r") as f:
        return yaml.safe_load(f)
    
def deep_merge(user: Dict[str, Any], default: Dict[str, Any]) -> Dict[str, Any]:
    user = dict(user)  
    for key, value in default.items():
        if key in user and isinstance(user[key], dict) and isinstance(value, dict):
            user[key] = deep_merge(user[key], value)
        else:
            user[key] = value
    return user




def render_templates(obj: Any, context: dict) -> Any:
    if isinstance(obj, dict):
        return {k: render_templates(v, context) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [render_templates(item, context) for item in obj]
    elif isinstance(obj, str):
        return Template(obj).render(**context)
    else:
        return obj


def camel_to_snake(name: str) -> str:
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def normalize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {camel_to_snake(k): v for k, v in d.items()}