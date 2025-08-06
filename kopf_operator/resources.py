from typing import Dict, Any, List
import logging
from kopf_operator.utils import normalize_keys
from kubernetes.client import (
    V1ObjectMeta, V1Deployment, V1DeploymentSpec, V1LabelSelector, V1PodTemplateSpec,
    V1PodSpec, V1Container, V1EnvVar, V1VolumeMount, V1ResourceRequirements, V1Probe,
    V1Volume, V1Service, V1ServiceSpec, V1ServicePort, V1ConfigMap, V1Secret,
    V1PersistentVolumeClaim, V1PersistentVolumeClaimSpec, V1Ingress, V1IngressSpec,
    V1IngressRule, V1HTTPIngressRuleValue, V1HTTPIngressPath, V1HorizontalPodAutoscaler,
    V1HorizontalPodAutoscalerSpec, V1TypedLocalObjectReference, V1Affinity, V1ExecAction,
    V1HTTPGetAction, V1TCPSocketAction, V1PersistentVolumeClaimVolumeSource, V1ConfigMapVolumeSource,
    V1SecretVolumeSource, V1IngressBackend, V1IngressServiceBackend, V1ServiceBackendPort
)


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def meta(name: str, namespace: str, labels: Dict[str, str]) -> V1ObjectMeta:
    logger.debug(f"Creating metadata: name={name}, namespace={namespace}, labels={labels}")
    return V1ObjectMeta(name=name, namespace=namespace, labels=labels)

class ResourceFactory:
    @staticmethod
    def labels(name: str) -> Dict[str, str]:
        labels = {"app": name}
        logger.debug(f"Generated labels: {labels}")
        return labels

    @staticmethod
    def deployment(name: str, ns: str, spec: Dict[str, Any]) -> V1Deployment:
        logger.debug(f"Creating Deployment for: {name} in namespace: {ns} with spec: {spec}")
        container_spec = spec.get("container", {})
        affinity = spec.get("affinity")

        volumes_spec = spec.get("volumes", [])

        def to_env_vars(env_list):
            from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector
            result = []
            for env in env_list:
                if "valueFrom" in env:
                    value_from = V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            name=env["valueFrom"]["secretKeyRef"]["name"],
                            key=env["valueFrom"]["secretKeyRef"]["key"]
                        )
                    )
                    result.append(V1EnvVar(name=env["name"], value_from=value_from))
                else:
                    result.append(V1EnvVar(name=env["name"], value=env["value"]))
            return result

        def to_volume_mounts(mounts_list):
            from kubernetes.client import V1VolumeMount

            result = []
            for mount in mounts_list:
                try:
                    norm = normalize_keys(mount)
                    result.append(V1VolumeMount(**norm))
                except TypeError as e:
                    logger.error(f"Invalid volumeMount entry: {mount}, error: {e}")
                    raise
            return result


        def to_volumes(volumes_list: List[Dict[str, Any]]) -> List[V1Volume]:
            result = []

            defined_volumes = {v["name"] for v in volumes_list}
            for mount in container_spec.get("volumeMounts", []):
                if mount["name"] not in defined_volumes:
                    logger.warning(f"Auto-adding emptyDir for missing volume: {mount['name']}")
                    volumes_list.append({"name": mount["name"], "emptyDir": {}})

            for v in volumes_list:
                try:
                    v = normalize_keys(v)

                    if "persistentVolumeClaim" in v or "persistent_volume_claim" in v:
                        pvc_data = v.pop("persistentVolumeClaim", v.pop("persistent_volume_claim", {}))
                        v["persistent_volume_claim"] = V1PersistentVolumeClaimVolumeSource(
                            claim_name=pvc_data.get("claimName") or pvc_data.get("claim_name")
                        )

                    if "config_map" in v:
                        cm = v.pop("config_map")
                        v["config_map"] = V1ConfigMapVolumeSource(
                            name=cm.get("name"),
                            items=cm.get("items")
                        )

                    if "secret" in v:
                        sec = v.pop("secret")
                        v["secret"] = V1SecretVolumeSource(
                            secret_name=sec.get("secret_name"),
                            items=sec.get("items")
                        )

                    result.append(V1Volume(**v))
                except Exception as e:
                    logger.error(f"Invalid volume entry: {v}, error: {e}")

            return result

        
        def normalize_probe(probe_dict: Dict[str, Any]) -> V1Probe:
            probe_dict = normalize_keys(probe_dict)

            if "exec" in probe_dict:
                exec_command = probe_dict.pop("exec", {}).get("command", [])
                probe_dict["_exec"] = V1ExecAction(command=exec_command)

            if "http_get" in probe_dict:
                http_get = probe_dict.pop("http_get", {})
                probe_dict["http_get"] = V1HTTPGetAction(
                    path=http_get.get("path", "/"),
                    port=http_get.get("port"),
                    host=http_get.get("host"),
                    scheme=http_get.get("scheme", "HTTP"),
                    http_headers=http_get.get("http_headers") 
                )

            if "tcp_socket" in probe_dict:
                tcp_socket = probe_dict.pop("tcp_socket", {})
                probe_dict["tcp_socket"] = V1TCPSocketAction(
                    port=tcp_socket.get("port"),
                    host=tcp_socket.get("host")
                )

            return V1Probe(**probe_dict)

        container = V1Container(
            name=container_spec.get("name", name),
            image=container_spec.get("image", "nginx"),
            ports=container_spec.get("ports", []),
            env=to_env_vars(container_spec.get("env", [])),
            volume_mounts=to_volume_mounts(container_spec.get("volumeMounts", [])),
            resources=V1ResourceRequirements(**container_spec.get("resources", {})),
            liveness_probe=normalize_probe(container_spec["livenessProbe"]) if "livenessProbe" in container_spec else None,
            readiness_probe=normalize_probe(container_spec["readinessProbe"]) if "readinessProbe" in container_spec else None

        )

        pod_spec = V1PodSpec(
            containers=[container],
            volumes=to_volumes(volumes_spec),
            affinity=V1Affinity(**normalize_keys(affinity)) if affinity else None
        )

        pod_template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels=ResourceFactory.labels(name)),
            spec=pod_spec
        )

        deployment_spec = V1DeploymentSpec(
            replicas=spec.get("replicas", 1),
            selector=V1LabelSelector(match_labels=ResourceFactory.labels(name)),
            template=pod_template
        )

        deployment = V1Deployment(
            metadata=meta(name, ns, ResourceFactory.labels(name)),
            spec=deployment_spec
        )

        logger.debug(f"Deployment created: {deployment}")
        return deployment



    @staticmethod
    def service(name: str, ns: str, spec: Dict[str, Any]) -> V1Service:
        logger.debug(f"Creating Service for: {name} in namespace: {ns} with spec: {spec}")
        svc_spec = spec.get("service", {})

        def normalize_port(p: Dict[str, Any]) -> V1ServicePort:
            port = dict(p)
            if "targetPort" in port:
                port["target_port"] = port.pop("targetPort")
            if "nodePort" in port:
                port["node_port"] = port.pop("nodePort")
            return V1ServicePort(**port)

        ports = [normalize_port(p) for p in svc_spec.get("ports", [{"port": 80, "targetPort": 80}])]

        service = V1Service(
            metadata=meta(f"{name}-svc", ns, ResourceFactory.labels(name)),
            spec=V1ServiceSpec(
                selector=svc_spec.get("selector", ResourceFactory.labels(name)),
                ports=ports,
                type=svc_spec.get("type", "ClusterIP")
            )
        )
        logger.debug(f"Service created: {service}")
        return service

    @staticmethod
    def configmap(name: str, ns: str, spec: Dict[str, Any]) -> V1ConfigMap:
        logger.debug(f"Creating ConfigMap for: {name} with spec: {spec}")
        configmap = V1ConfigMap(
            metadata=meta(f"{name}-config", ns, ResourceFactory.labels(name)),
            data=spec.get("data", {})
        )
        logger.debug(f"ConfigMap created: {configmap}")
        return configmap

    @staticmethod
    def secret(name: str, ns: str, spec: Dict[str, Any]) -> V1Secret:
        logger.debug(f"Creating Secret for: {name} with spec: {spec}")
        secret = V1Secret(
            metadata=meta(f"{name}-secrets", ns, ResourceFactory.labels(name)),
            string_data=spec.get("data", {}),
            type=spec.get("type", "Opaque")
        )
        logger.debug(f"Secret created: {secret}")
        return secret

    @staticmethod
    def pvc(name: str, ns: str, spec: Dict[str, Any]) -> V1PersistentVolumeClaim:
        logger.debug(f"Creating PVC for: {name} with spec: {spec}")
        pvc = V1PersistentVolumeClaim(
            metadata=meta(f"{name}-pvc", ns, ResourceFactory.labels(name)),
            spec = V1PersistentVolumeClaimSpec(**normalize_keys(spec))
        )
        logger.debug(f"PVC created: {pvc}")
        return pvc

    @staticmethod
    def ingress(name: str, ns: str, spec: dict) -> V1Ingress:
        paths_spec = spec.get("paths", [])
        if isinstance(paths_spec, dict):
            paths_spec = [paths_spec] 

        normalized_paths = []

        for p in paths_spec:
            p = normalize_keys(p)  
            backend_spec = normalize_keys(p.get("backend", {}))

            backend = None
            if "service" in backend_spec:
                service_spec = backend_spec["service"]
                port_spec = service_spec.get("port", {})
                service_backend_port = None

                if "number" in port_spec:
                    service_backend_port = V1ServiceBackendPort(number=port_spec["number"])
                elif "name" in port_spec:
                    service_backend_port = V1ServiceBackendPort(name=port_spec["name"])

                backend = V1IngressBackend(
                    service=V1IngressServiceBackend(
                        name=service_spec.get("name"),
                        port=service_backend_port
                    )
                )
            elif "resource" in backend_spec:
                backend = V1IngressBackend(resource=backend_spec["resource"])

            normalized_paths.append(
                V1HTTPIngressPath(
                    path=p.get("path"),
                    path_type=p.get("path_type", "Prefix"),
                    backend=backend
                )
            )

        return V1Ingress(
            metadata=meta(f"{name}-ingress", ns, ResourceFactory.labels(name)),
            spec=V1IngressSpec(
                rules=[
                    V1IngressRule(
                        host=spec.get("host", f"{name}.local"),
                        http=V1HTTPIngressRuleValue(paths=normalized_paths)
                    )
                ]
            )
        )



    @staticmethod
    def hpa(name: str, ns: str, spec: Dict[str, Any]) -> V1HorizontalPodAutoscaler:
        logger.debug(f"Creating HPA for: {name} with spec: {spec}")
        hpa = V1HorizontalPodAutoscaler(
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
        logger.debug(f"HPA created: {hpa}")
        return hpa
