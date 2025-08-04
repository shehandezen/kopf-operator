from typing import Dict, Any
from kubernetes.client import *

def meta(name: str, namespace: str, labels: Dict[str, str]) -> V1ObjectMeta:
    return V1ObjectMeta(name=name, namespace=namespace, labels=labels)

class ResourceFactory:
    @staticmethod
    def labels(name: str) -> Dict[str, str]:
        return {"app": name}

    @staticmethod
    def deployment(name: str, ns: str, spec: Dict[str, Any]) -> V1Deployment:
        container_spec = spec.get("container", {})
        volumes = container_spec.get("volumes", [])
        affinity = spec.get("affinity")

        return V1Deployment(
            metadata=meta(name, ns, ResourceFactory.labels(name)),
            spec=V1DeploymentSpec(
                replicas=spec.get("replicas", 1),
                selector=V1LabelSelector(match_labels=ResourceFactory.labels(name)),
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels=ResourceFactory.labels(name)),
                    spec=V1PodSpec(
                        containers=[V1Container(
                            name=container_spec.get("name", name),
                            image=container_spec.get("image", "nginx"),
                            ports=container_spec.get("ports", []),
                            env=[V1EnvVar(**e) for e in container_spec.get("env", [])],
                            volume_mounts=[V1VolumeMount(**vm) for vm in container_spec.get("volumeMounts", [])],
                            resources=V1ResourceRequirements(
                                **container_spec.get("resources", {})
                            ),
                            liveness_probe=V1Probe(**container_spec.get("livenessProbe", {})) if container_spec.get("livenessProbe") else None,
                            readiness_probe=V1Probe(**container_spec.get("readinessProbe", {})) if container_spec.get("readinessProbe") else None
                        )],
                        volumes=[V1Volume(**v) for v in volumes],
                        affinity=V1Affinity(**affinity) if affinity else None
                    )
                )
            )
        )

    @staticmethod
    def service(name: str, ns: str, spec: Dict[str, Any]) -> V1Service:
        svc_spec = spec.get("service", {})
        return V1Service(
            metadata=meta(f"{name}-svc", ns, ResourceFactory.labels(name)),
            spec=V1ServiceSpec(
                selector=svc_spec.get("selector", ResourceFactory.labels(name)),
                ports=[V1ServicePort(**p) for p in svc_spec.get("ports", [{"port": 80, "targetPort": 80}])],
                type=svc_spec.get("type", "ClusterIP")
            )
        )

    @staticmethod
    def configmap(name: str, ns: str, spec: Dict[str, Any]) -> V1ConfigMap:
        return V1ConfigMap(
            metadata=meta(f"{name}-config", ns, ResourceFactory.labels(name)),
            data=spec.get("data", {})
        )

    @staticmethod
    def secret(name: str, ns: str, spec: Dict[str, Any]) -> V1Secret:
        return V1Secret(
            metadata=meta(f"{name}-secret", ns, ResourceFactory.labels(name)),
            string_data=spec.get("data", {}),
            type=spec.get("type", "Opaque")
        )

    @staticmethod
    def pvc(name: str, ns: str, spec: Dict[str, Any]) -> V1PersistentVolumeClaim:
        return V1PersistentVolumeClaim(
            metadata=meta(f"{name}-pvc", ns, ResourceFactory.labels(name)),
            spec=V1PersistentVolumeClaimSpec(**spec)
        )

    @staticmethod
    def ingress(name: str, ns: str, spec: Dict[str, Any]) -> V1Ingress:
        paths = spec.get("paths", [{
            "path": "/",
            "pathType": "Prefix",
            "backend": {
                "service": {
                    "name": name,
                    "port": {"number": 80}
                }
            }
        }])
        return V1Ingress(
            metadata=meta(f"{name}-ingress", ns, ResourceFactory.labels(name)),
            spec=V1IngressSpec(
                rules=[V1IngressRule(
                    host=spec.get("host", f"{name}.local"),
                    http=V1HTTPIngressRuleValue(
                        paths=[V1HTTPIngressPath(**p) for p in paths]
                    )
                )]
            )
        )

    @staticmethod
    def hpa(name: str, ns: str, spec: Dict[str, Any]) -> V1HorizontalPodAutoscaler:
        return V1HorizontalPodAutoscaler(
            metadata=meta(f"{name}-hpa", ns, ResourceFactory.labels(name)),
            spec=V1HorizontalPodAutoscalerSpec(
                scale_target_ref=V1TypedLocalObjectReference(
                    api_version="apps/v1",
                    kind="Deployment",
                    name=name
                ),
                min_replicas=spec.get("minReplicas", 1),
                max_replicas=spec.get("maxReplicas", 5),
                target_cpu_utilization_percentage=spec.get("cpuUtilization", 50)
            )
        )