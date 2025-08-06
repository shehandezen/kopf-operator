import logging
import os
import kopf
import kubernetes
from kopf_operator.base import BaseKopfOperator
import logging

logging.basicConfig(level=logging.DEBUG)

try:
    kubernetes.config.load_incluster_config()
except kubernetes.config.ConfigException:
    kubernetes.config.load_kube_config()

kind =  os.getenv("OPERATOR_KIND")
plural = os.getenv("OPERATOR_PLURAL")
group = os.getenv("OPERATOR_GROUP")
version = os.getenv("OPERATOR_VERSION")

operator = BaseKopfOperator(
    kind=kind,
    plural=plural,
    group=group,
    version=version
)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    print("✅ Kopf Operator Started.")
    settings.posting.level = 20
    settings.watching.server_timeout = 60
    settings.watching.client_timeout = 90
    settings.watching.connect_timeout = 10
    settings.watching.namespaces = None

operator.register(kopf)
