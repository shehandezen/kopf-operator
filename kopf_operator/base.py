import kopf
import kubernetes.client as k8s
from kubernetes.client.rest import ApiException
from typing import Any, Dict
from kopf import HandlerRegistry
from .resources import ResourceFactory
from .utils import deep_merge

class BaseKopfOperator:
    group: str = "cneura.ai"
    version: str = "v1alpha1"
    plural: str = "cneurapps"
    kind: str = "CneurApp"

    def __init__(self, kind: str, plural: str, group: str = "cneura.ai", version: str = "v1alpha1"):
        self.kind = kind
        self.plural = plural
        self.group = group
        self.version = version
        self.core_v1 = k8s.CoreV1Api()
        self.apps_v1 = k8s.AppsV1Api()
        self.networking_v1 = k8s.NetworkingV1Api()
        self.autoscaling_v1 = k8s.AutoscalingV1Api()
        self.api = k8s.CustomObjectsApi()

    def register(self, registry: HandlerRegistry):
        @registry.on.create(self.group, self.version, self.plural)
        def on_create(spec, name, namespace, **kwargs):
            self.log(f"[CREATE] {name} in {namespace}")
            self.create_all_resources(name, namespace, spec)
            return {"status": "created"}

        @registry.on.update(self.group, self.version, self.plural)
        def on_update(spec, name, namespace, **kwargs):
            self.log(f"[UPDATE] {name} in {namespace}")
            self.update_all_resources(name, namespace, spec)
            return {"status": "updated"}

        @registry.on.delete(self.group, self.version, self.plural)
        def on_delete(spec, name, namespace, **kwargs):
            self.log(f"[DELETE] {name} in {namespace}")
            self.delete_all_resources(name, namespace)
            return {"status": "deleted"}

        @registry.timer(self.group, self.version, self.plural, interval=60.0)
        def reconcile_every_minute(spec, name, namespace, **kwargs):
            self.log(f"[RECONCILE] {name} in {namespace}")
            self.reconcile(name, namespace, spec)

    def create_all_resources(self, name: str, ns: str, spec: Dict[str, Any]):
        # Create resources if specified in spec
        self.apply_resource(self.apps_v1.create_namespaced_deployment, ns, ResourceFactory.deployment(name, ns, spec))
        self.apply_resource(self.core_v1.create_namespaced_service, ns, ResourceFactory.service(name, ns, spec))

        if "configmap" in spec:
            self.apply_resource(self.core_v1.create_namespaced_config_map, ns, ResourceFactory.configmap(name, ns, spec["configmap"]))

        if "secret" in spec:
            self.apply_resource(self.core_v1.create_namespaced_secret, ns, ResourceFactory.secret(name, ns, spec["secret"]))

        if "pvc" in spec:
            self.apply_resource(self.core_v1.create_namespaced_persistent_volume_claim, ns, ResourceFactory.pvc(name, ns, spec["pvc"]))

        if "ingress" in spec:
            self.apply_resource(self.networking_v1.create_namespaced_ingress, ns, ResourceFactory.ingress(name, ns, spec["ingress"]))

        if "hpa" in spec:
            self.apply_resource(self.autoscaling_v1.create_namespaced_horizontal_pod_autoscaler, ns, ResourceFactory.hpa(name, ns, spec["hpa"]))

    def update_all_resources(self, name: str, ns: str, spec: Dict[str, Any]):
        # Patch existing resources to desired state
        self.apply_resource(self.apps_v1.patch_namespaced_deployment, ns, ResourceFactory.deployment(name, ns, spec), name)
        self.apply_resource(self.core_v1.patch_namespaced_service, ns, ResourceFactory.service(name, ns, spec), name)

        if "configmap" in spec:
            self.apply_resource(self.core_v1.patch_namespaced_config_map, ns, ResourceFactory.configmap(name, ns, spec["configmap"]), name)

        if "secret" in spec:
            self.apply_resource(self.core_v1.patch_namespaced_secret, ns, ResourceFactory.secret(name, ns, spec["secret"]), name)

        if "pvc" in spec:
            self.apply_resource(self.core_v1.patch_namespaced_persistent_volume_claim, ns, ResourceFactory.pvc(name, ns, spec["pvc"]), name)

        if "ingress" in spec:
            self.apply_resource(self.networking_v1.patch_namespaced_ingress, ns, ResourceFactory.ingress(name, ns, spec["ingress"]), name)

        if "hpa" in spec:
            self.apply_resource(self.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler, ns, ResourceFactory.hpa(name, ns, spec["hpa"]), name)

    def delete_all_resources(self, name: str, ns: str):
        delete_opts = k8s.V1DeleteOptions()
        # Delete in reverse order of creation (to handle dependencies)
        resource_deletors = [
            (self.autoscaling_v1.delete_namespaced_horizontal_pod_autoscaler, name),
            (self.networking_v1.delete_namespaced_ingress, name),
            (self.core_v1.delete_namespaced_persistent_volume_claim, name),
            (self.core_v1.delete_namespaced_secret, name),
            (self.core_v1.delete_namespaced_config_map, name),
            (self.core_v1.delete_namespaced_service, name),
            (self.apps_v1.delete_namespaced_deployment, name),
        ]
        for deleter, res_name in resource_deletors:
            try:
                deleter(name=res_name, namespace=ns, body=delete_opts)
                self.log(f"Deleted resource {res_name}")
            except ApiException as e:
                if e.status != 404:
                    self.log(f"Error deleting resource {res_name}: {e}")

    def reconcile(self, name: str, ns: str, spec: Dict[str, Any]):
        # Reconcile each resource: recreate if missing or patch if drifted
        self._reconcile_resource(
            resource_name=name,
            namespace=ns,
            read_fn=self.apps_v1.read_namespaced_deployment,
            patch_fn=self.apps_v1.patch_namespaced_deployment,
            desired=ResourceFactory.deployment(name, ns, spec),
            drift_checker=self._deployment_drifted
        )

        self._reconcile_resource(
            resource_name=name,
            namespace=ns,
            read_fn=self.core_v1.read_namespaced_service,
            patch_fn=self.core_v1.patch_namespaced_service,
            desired=ResourceFactory.service(name, ns, spec),
            drift_checker=self._simple_metadata_drifted
        )

        if "configmap" in spec:
            self._reconcile_resource(
                resource_name=name,
                namespace=ns,
                read_fn=self.core_v1.read_namespaced_config_map,
                patch_fn=self.core_v1.patch_namespaced_config_map,
                desired=ResourceFactory.configmap(name, ns, spec["configmap"]),
                drift_checker=self._simple_metadata_drifted
            )

        if "secret" in spec:
            self._reconcile_resource(
                resource_name=name,
                namespace=ns,
                read_fn=self.core_v1.read_namespaced_secret,
                patch_fn=self.core_v1.patch_namespaced_secret,
                desired=ResourceFactory.secret(name, ns, spec["secret"]),
                drift_checker=self._simple_metadata_drifted
            )

        if "pvc" in spec:
            self._reconcile_resource(
                resource_name=name,
                namespace=ns,
                read_fn=self.core_v1.read_namespaced_persistent_volume_claim,
                patch_fn=self.core_v1.patch_namespaced_persistent_volume_claim,
                desired=ResourceFactory.pvc(name, ns, spec["pvc"]),
                drift_checker=self._simple_metadata_drifted
            )

        if "ingress" in spec:
            self._reconcile_resource(
                resource_name=name,
                namespace=ns,
                read_fn=self.networking_v1.read_namespaced_ingress,
                patch_fn=self.networking_v1.patch_namespaced_ingress,
                desired=ResourceFactory.ingress(name, ns, spec["ingress"]),
                drift_checker=self._simple_metadata_drifted
            )

        if "hpa" in spec:
            self._reconcile_resource(
                resource_name=name,
                namespace=ns,
                read_fn=self.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler,
                patch_fn=self.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler,
                desired=ResourceFactory.hpa(name, ns, spec["hpa"]),
                drift_checker=self._simple_metadata_drifted
            )

    def _reconcile_resource(self, resource_name, namespace, read_fn, patch_fn, desired, drift_checker):
        try:
            current = read_fn(name=resource_name, namespace=namespace)
            if drift_checker(current, desired):
                self.log(f"Resource {resource_name} in {namespace} drift detected, patching...")
                patch_fn(name=resource_name, namespace=namespace, body=desired)
            else:
                self.log(f"Resource {resource_name} in {namespace} is up-to-date")
        except ApiException as e:
            if e.status == 404:
                self.log(f"Resource {resource_name} in {namespace} not found, creating...")
                self.apply_resource(lambda namespace, body: patch_fn(resource_name, namespace, body), namespace, desired)
            else:
                self.log(f"Error reconciling resource {resource_name}: {e}")

    def _deployment_drifted(self, current, desired):
        # Check replicas drift
        desired_replicas = desired.spec.replicas
        current_replicas = current.status.ready_replicas or 0
        if desired_replicas != current_replicas:
            return True
        # You can add more advanced checks (e.g. container image, env, probes) here
        return False

    def _simple_metadata_drifted(self, current, desired):
        # Compare labels and annotations (simple drift detection)
        if current.metadata.labels != desired.metadata.labels:
            return True
        if current.metadata.annotations != desired.metadata.annotations:
            return True
        return False

    def apply_resource(self, method, ns: str, body: Any, name: str = None):
        try:
            if name:
                method(name=name, namespace=ns, body=body)
            else:
                method(namespace=ns, body=body)
            self.log(f"Applied resource {body.metadata.name}")
        except ApiException as e:
            self.log(f"K8s API error: {e}")

    def log(self, message: str):
        print(message)
