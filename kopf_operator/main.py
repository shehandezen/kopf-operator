import kopf
import kubernetes
from kopf_operator.base import BaseKopfOperator

def main():
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()

    operator = BaseKopfOperator(
        kind="CneurApp",
        plural="cneurapps",
        group="cneura.ai",
        version="v1alpha1"
    )

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.level = "INFO"

    operator.register(kopf)
    kopf.run()  

if __name__ == "__main__":
    main()
