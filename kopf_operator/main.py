import os
import kopf
import kubernetes
from kopf_operator.base import BaseKopfOperator

def main():
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()

    kind = os.getenv("OPERATOR_KIND", "CneurApp")
    plural = os.getenv("OPERATOR_PLURAL", "cneurapps")
    group = os.getenv("OPERATOR_GROUP", "cneura.ai")
    version = os.getenv("OPERATOR_VERSION", "v1alpha1")

    operator = BaseKopfOperator(
        kind=kind,
        plural=plural,
        group=group,
        version=version
    )

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.level = "INFO"

    operator.register(kopf)
    kopf.run()

if __name__ == "__main__":
    main()
