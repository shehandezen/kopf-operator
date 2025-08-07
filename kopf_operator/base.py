from copy import deepcopy
import kopf
import logging
import kubernetes.client as k8s
from kubernetes.client.rest import ApiException
from typing import Any, Dict
from kopf_operator.resources import ResourceFactory
from kopf_operator.utils import load_defaults, deep_merge, render_templates

logger = logging.getLogger("kopf-operator")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class BaseKopfOperator:
    def __init__(self, kind: str, plural: str, group: str = "cneura.ai", version: str = "v1"):
        self.kind = kind
        self.plural = plural
        self.group = group
        self.version = version
        self.core_v1 = k8s.CoreV1Api()
        self.apps_v1 = k8s.AppsV1Api()
        self.batch_v1 = k8s.BatchV1Api()
        self.networking_v1 = k8s.NetworkingV1Api()
        self.autoscaling_v1 = k8s.AutoscalingV1Api()
        self.api = k8s.CustomObjectsApi()

    def register(self, kopf_module):
        @kopf_module.on.create(self.group, self.version, self.plural)
        def on_create(spec, name, namespace, **kwargs):
            self.log(f"[CREATE] {name} in {namespace}")
            self.create_all_resources(name, namespace, spec)
            return {"status": "created"}

        @kopf_module.on.update(self.group, self.version, self.plural)
        def on_update(spec, name, namespace, **kwargs):
            self.log(f"[UPDATE] {name} in {namespace}")
            self.update_all_resources(name, namespace, spec)
            return {"status": "updated"}

        @kopf_module.on.delete(self.group, self.version, self.plural)
        def on_delete(spec, name, namespace, **kwargs):
            self.log(f"[DELETE] {name} in {namespace}")
            self.delete_all_resources(name, namespace)
            return {"status": "deleted"}

        @kopf_module.timer(self.group, self.version, self.plural, interval=60.0)
        def reconcile_every_minute(spec, name, namespace, **kwargs):
            self.log(f"[RECONCILE] {name} in {namespace}")
            self.reconcile(name, namespace, spec)

    def apply_runtime_defaults(self, kind: str, name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        spec = deepcopy(dict(spec)) 
        self.log(f"[DEBUG] user spec: {spec}")
        defaults = load_defaults(kind)
        rendered = render_templates(defaults, {"name": name})
        self.log(f"[DEBUG] default spec: {rendered}")
        return deep_merge(spec, rendered.get("spec"))

    def create_all_resources(self, name: str, ns: str, spec: Dict[str, Any]):
        self.log(f"Creating all resources for {name} in {ns}")
        spec = self.apply_runtime_defaults(kind=self.kind, name=name, spec=spec)
        if "configmap" in spec:
            self.apply_resource(self.core_v1.create_namespaced_config_map, ns, ResourceFactory.configmap(name, ns, spec["configmap"]))

        if "secret" in spec:
            self.apply_resource(self.core_v1.create_namespaced_secret, ns, ResourceFactory.secret(name, ns, spec["secret"]))

        self.apply_resource(self.core_v1.create_namespaced_service, ns, ResourceFactory.service(name, ns, spec))
        
        if "stateful" in spec:
            self.apply_resource(self.apps_v1.create_namespaced_stateful_set, ns, ResourceFactory.statefulset(name, ns, spec["stateful"]), f"{name}-stateful")
            self.apply_resource(self.core_v1.create_namespaced_service, ns, ResourceFactory.service(name, ns, spec, True))
        else: 
            if "pvc" in spec:
                self.apply_resource(self.core_v1.create_namespaced_persistent_volume_claim, ns, ResourceFactory.pvc(name, ns, spec["pvc"]))

            self.apply_resource(self.apps_v1.create_namespaced_deployment, ns, ResourceFactory.deployment(name, ns, spec))
            self.apply_resource(self.core_v1.create_namespaced_service, ns, ResourceFactory.service(name, ns, spec))

        if "pod" in spec:
            self.apply_resource(self.core_v1.create_namespaced_pod, ns, ResourceFactory.pod(name, ns, spec["pod"]))

        if "job" in spec:
            self.apply_resource(self.batch_v1.create_namespaced_job,ns,ResourceFactory.job(name, ns, spec["job"]))

        if "cronjob" in spec:
            self.apply_resource(self.batch_v1.create_namespaced_cron_job,ns,ResourceFactory.cronjob(name, ns, spec["cronjob"]))
            
        if "ingress" in spec:
            self.apply_resource(self.networking_v1.create_namespaced_ingress, ns, ResourceFactory.ingress(name, ns, spec["ingress"]))

        if "hpa" in spec:
            self.apply_resource(self.autoscaling_v1.create_namespaced_horizontal_pod_autoscaler, ns, ResourceFactory.hpa(name, ns, spec["hpa"]))

    def update_all_resources(self, name: str, ns: str, spec: Dict[str, Any]):
        self.log(f"Updating all resources for {name} in {ns}")
        spec = self.apply_runtime_defaults(kind=self.kind, name=name, spec=spec)
        if "configmap" in spec:
            self.apply_resource(self.core_v1.patch_namespaced_config_map, ns, ResourceFactory.configmap(name, ns, spec["configmap"]), name)

        if "secret" in spec:
            self.apply_resource(self.core_v1.patch_namespaced_secret, ns, ResourceFactory.secret(name, ns, spec["secret"]), name)


        if "stateful" in spec:
            self.apply_resource(self.apps_v1.patch_namespaced_stateful_set, ns, ResourceFactory.statefulset(name, ns, spec["stateful"]), f"{name}-stateful")
            self.apply_resource(self.core_v1.patch_namespaced_service, ns, ResourceFactory.service(name, ns, spec, True), f"{name}-svc")
        else:
            if "pvc" in spec:
                self.apply_resource(self.core_v1.patch_namespaced_persistent_volume_claim, ns, ResourceFactory.pvc(name, ns, spec["pvc"]), name)
                
            self.apply_resource(self.apps_v1.patch_namespaced_deployment, ns, ResourceFactory.deployment(name, ns, spec), name)
            self.apply_resource(self.core_v1.patch_namespaced_service, ns, ResourceFactory.service(name, ns, spec), f"{name}-svc")

        if "pod" in spec:
            self.apply_resource(self.core_v1.patch_namespaced_pod, ns, ResourceFactory.pod(name, ns, spec["pod"]))

        if "job" in spec:
            self.apply_resource(self.batch_v1.patch_namespaced_job,ns,ResourceFactory.job(name, ns, spec["job"]))

        if "cronjob" in spec:
            self.apply_resource(self.batch_v1.patch_namespaced_cron_job,ns,ResourceFactory.cronjob(name, ns, spec["cronjob"]))
        
        if "ingress" in spec:
            self.apply_resource(self.networking_v1.patch_namespaced_ingress, ns, ResourceFactory.ingress(name, ns, spec["ingress"]), name)

        if "hpa" in spec:
            self.apply_resource(self.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler, ns, ResourceFactory.hpa(name, ns, spec["hpa"]), name)

    def delete_all_resources(self, name: str, ns: str):
        self.log(f"Deleting all resources for {name} in {ns}")
        delete_opts = k8s.V1DeleteOptions()
        resource_deletors = [
            (self.autoscaling_v1.delete_namespaced_horizontal_pod_autoscaler, f"{name}-hpa"),
            (self.networking_v1.delete_namespaced_ingress, f"{name}-ingress"),
            (self.core_v1.delete_namespaced_persistent_volume_claim, f"{name}-pvc"),
            (self.core_v1.delete_namespaced_secret, f"{name}-secrets"),
            (self.core_v1.delete_namespaced_config_map, f"{name}-config"),
            (self.core_v1.delete_namespaced_service, f"{name}-svc"),
            (self.apps_v1.delete_namespaced_deployment, name),
            (self.apps_v1.delete_namespaced_stateful_set, f"{name}-stateful"),
            (self.core_v1.delete_namespaced_pod, f"{name}-pod"),
            (self.batch_v1.delete_namespaced_job, f"{name}-job"),
            (self.batch_v1.delete_namespaced_cron_job, f"{name}-cron-job"),
        ]
        for deleter, res_name in resource_deletors:
            try:
                deleter(name=res_name, namespace=ns, body=delete_opts)
                self.log(f"✅ Deleted resource: {res_name}")
            except ApiException as e:
                if e.status != 404:
                    self.log(f"❌ Error deleting {res_name}: {e}")

    def reconcile(self, name: str, ns: str, spec: Dict[str, Any]):
        self.log(f"Reconciling resources for {name} in {ns}")
        spec = self.apply_runtime_defaults(kind=self.kind, name=name, spec=spec)

        self._reconcile_resource(
            resource_name=name,
            namespace=ns,
            read_fn=self.apps_v1.read_namespaced_deployment,
            patch_fn=self.apps_v1.patch_namespaced_deployment,
            desired=ResourceFactory.deployment(name, ns, spec),
            drift_checker=self._deployment_drifted
        )

        self._reconcile_resource(
            resource_name=f"{name}-svc",
            namespace=ns,
            read_fn=self.core_v1.read_namespaced_service,
            patch_fn=self.core_v1.patch_namespaced_service,
            desired=ResourceFactory.service(name, ns, spec),
            drift_checker=self._service_drifted
        )

        optional_resources = {
            "configmap": (f"{name}-config", self.core_v1.read_namespaced_config_map, self.core_v1.patch_namespaced_config_map, ResourceFactory.configmap),
            "secret": (f"{name}-secrets", self.core_v1.read_namespaced_secret, self.core_v1.patch_namespaced_secret, ResourceFactory.secret),
            "pvc": (f"{name}-pvc", self.core_v1.read_namespaced_persistent_volume_claim, self.core_v1.patch_namespaced_persistent_volume_claim, ResourceFactory.pvc),
            "ingress": (f"{name}-ingress", self.networking_v1.read_namespaced_ingress, self.networking_v1.patch_namespaced_ingress, ResourceFactory.ingress),
            "hpa": (f"{name}-hpa", self.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler, self.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler, ResourceFactory.hpa),
            "pod": (f"{name}-pod", self.core_v1.read_namespaced_pod, self.core_v1.patch_namespaced_pod, ResourceFactory.pod),
            "pod": (f"{name}-stateful", self.apps_v1.read_namespaced_stateful_set, self.apps_v1.patch_namespaced_stateful_set, ResourceFactory.statefulset),
            "job": (f"{name}-job", self.batch_v1.read_namespaced_job, self.batch_v1.patch_namespaced_job, ResourceFactory.job),
            "cron-job": (f"{name}-cron-job", self.batch_v1.read_namespaced_cron_job, self.batch_v1.patch_namespaced_cron_job, ResourceFactory.pvc),
        }

        for key, (resource_name, read_fn, patch_fn, factory_fn) in optional_resources.items():
            if key in spec:
                self._reconcile_resource(
                    resource_name=resource_name,
                    namespace=ns,
                    read_fn=read_fn,
                    patch_fn=patch_fn,
                    desired=factory_fn(name, ns, spec[key]),
                    drift_checker=self._simple_metadata_drifted
                )

    def _reconcile_resource(self, resource_name, namespace, read_fn, patch_fn, desired, drift_checker):
        try:
            current = read_fn(name=resource_name, namespace=namespace)
            if drift_checker(current, desired):
                self.log(f"⚠️ Drift detected in {resource_name}, patching...")
                patch_fn(name=resource_name, namespace=namespace, body=desired)
            else:
                self.log(f"✅ {resource_name} is up to date.")
        except ApiException as e:
            if e.status == 404:
                self.log(f"🔄 {resource_name} not found. Creating...")
                self.apply_resource(
                    lambda **kwargs: patch_fn(name=resource_name, **kwargs),
                    namespace,
                    desired
                )
            else:
                self.log(f"❌ Error reading {resource_name}: {e}")

    def _deployment_drifted(self, current, desired):
        desired_replicas = desired.spec.replicas
        current_replicas = current.status.ready_replicas or 0
        if desired_replicas != current_replicas:
            return True
        return False

    def _service_drifted(self, current, desired):
        def sort_ports(ports):
            return sorted([f"{p.port}:{p.target_port}" for p in ports])

        if current.spec.selector != desired.spec.selector:
            return True
        if sort_ports(current.spec.ports) != sort_ports(desired.spec.ports):
            return True
        return self._simple_metadata_drifted(current, desired)

    def _simple_metadata_drifted(self, current, desired):
        return current.metadata.labels != desired.metadata.labels or \
               current.metadata.annotations != desired.metadata.annotations

    def apply_resource(self, method, ns: str, body: Any, name: str = None):
        try:
            if name:
                method(name=name, namespace=ns, body=body)
            else:
                method(namespace=ns, body=body)
            self.log(f"✅ Applied resource: {body.metadata.name}")
        except ApiException as e:
            if e.status == 409:
                self.log(f"⚠️ Resource {body.metadata.name} already exists.")
            else:
                self.log(f"❌ API Error applying {body.metadata.name}: {e}")

    def log(self, message: str):
        logger.debug(message)
