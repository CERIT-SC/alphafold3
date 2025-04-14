import datetime
from threading import Lock
import time
import kubernetes
import kubernetes.config
import kubernetes.client
import kubernetes.utils.quantity

class KubernetesExecutor:
  def __init__(self, metrics_output: str = ""):
    kubernetes.config.load_incluster_config()
    self.api = kubernetes.client.BatchV1Api()
    self.kubeapi = kubernetes.client.CoreV1Api()
    self.custom_api = kubernetes.client.CustomObjectsApi()
    self.metrics_output = metrics_output
    self.metrics_output_lock = Lock()

  def wait_for_job_to_finish(self, job: tuple[str, str], print_logs: bool = False, print_metrics: bool = False) -> None:
    job_id, namespace = job
    printed_log = ""

    retry_counter = 0
    metrics_mod_counter = 0

    while True:
      try:
        current_job = self.api.read_namespaced_job(job_id, namespace)
      except kubernetes.client.rest.ApiException as e:
        if e.status == 404 or e.status == 500:
          time.sleep(6)
          retry_counter += 1
          if retry_counter > 10:
            raise e
          
          continue
        else:
          raise e

      retry_counter = 0
      pod = self.get_job_pod(job)

      if print_logs:
        printed_log = self._print_new_logs(job, printed_log)

      # Print metrics every 30 seconds
      if print_metrics and self.metrics_output and metrics_mod_counter % 6 == 0:
        if pod is not None:
          cpu_usage, memory_usage = self._get_pod_metrics(pod)
          if cpu_usage and memory_usage:
            with self.metrics_output_lock:
              with open(self.metrics_output, "a") as f:
                f.write(f"{datetime.datetime.now()} {job_id} {cpu_usage} {memory_usage} \n")

        metrics_mod_counter = 0

      if current_job.status.succeeded is not None:
        break

      if current_job.status.failed is not None:
        # Check if OOMKilled
        if pod is not None:
          for container in pod.status.container_statuses:
            if container.state.terminated.reason == "OOMKilled":
              raise RuntimeError(f"Job {job_id} OOMKilled")
        
        raise RuntimeError(f"Job {job_id} failed")
        
      metrics_mod_counter += 1
      time.sleep(5)

  def _print_new_logs(self, job: tuple[str, str], printed_log: str) -> str:
    full_log = self.get_job_logs(job) or ""
    if full_log.startswith(printed_log):
      new_log = full_log[len(printed_log):]
    else:
      new_log = full_log

    if new_log:
      print(new_log, end="", flush=True)
      printed_log += new_log

    return printed_log
  
  def _get_pod_metrics(self, pod):
    try:
      api_response = self.custom_api.get_namespaced_custom_object(
        group="metrics.k8s.io",
        version="v1beta1",
        namespace=pod.metadata.namespace,
        plural="pods",
        name=pod.metadata.name
      )
    except kubernetes.client.rest.ApiException as e:
      if e.status != 404:
        print(e)
      return None, None

    cpu_usage = api_response["containers"][0]["usage"]["cpu"]
    memory_usage = api_response["containers"][0]["usage"]["memory"]

    parsed_cpu_usage = kubernetes.utils.quantity.parse_quantity(cpu_usage)
    parsed_memory_usage = kubernetes.utils.quantity.parse_quantity(memory_usage)

    return (parsed_cpu_usage, parsed_memory_usage)
  
  def delete_job(self, job: tuple[str, str]):
    job_id, namespace = job
    try:
      self.api.delete_namespaced_job(job_id, namespace, propagation_policy="Foreground")
    except kubernetes.client.rest.ApiException as e:
      if e.status != 404:
        raise
    
  def parse_pvc_mounts(self, pvc_mounts_str: str) -> list[dict[str, str]]:
    pvc_mounts = pvc_mounts_str.split(',')
    parsed_pvc_mounts = [pvc_mount.split(':') for pvc_mount in pvc_mounts]
    return [{'claim_name': pvc_mount[0], 'mount_path': pvc_mount[1]} for pvc_mount in parsed_pvc_mounts]
  
  def get_job_pod(self, job):
    jobid, namespace = job
    pods = self.kubeapi.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"job-name={jobid}"
    )
    if len(pods.items) == 0:
        return None
    return pods.items[0]

  def get_job_logs(self, job: tuple[str, str]) -> str:
    _, namespace = job
    pod = self.get_job_pod(job)

    if pod is None:
      return ""
    
    try:
      pod_log = self.kubeapi.read_namespaced_pod_log(
          name=pod.metadata.name,
          namespace=namespace
      )
    except kubernetes.client.rest.ApiException as e:
      return ""
    
    return pod_log


  def create_job(
      self, 
      name: str,
      namespace: str,
      labels: dict[str, str],
      image: str, 
      command: list[str],
      args: list[str], 
      pvc_mounts: list[dict[str, str]],
      service_account: str | None = None,
      env: list[dict[str, str]] | None = None,
      gpu: bool = False,
      gpu_mem: str = "40000",
      cpu: tuple[str, str] | None = None,
      memory: tuple[str, str] | None = None
    ) -> tuple[str, str]:
    cpu = cpu or ("8", "16")
    memory = memory or ("32Gi", "64Gi") 

    volumes = []
    volume_mounts = []

    for pvc_mount in pvc_mounts:
      volumes.append(
        kubernetes.client.V1Volume(
          name=pvc_mount["claim_name"],
          persistent_volume_claim=kubernetes.client.V1PersistentVolumeClaimVolumeSource(
            claim_name=pvc_mount["claim_name"]
          ),
        )
      )

      volume_mounts.append(
        kubernetes.client.V1VolumeMount(
          name=pvc_mount["claim_name"], mount_path=pvc_mount["mount_path"]
        )
      )

    volumes.append(
      kubernetes.client.V1Volume(
        name="dshm",
        empty_dir=kubernetes.client.V1EmptyDirVolumeSource(
          medium="Memory", size_limit="120Gi"
        ),
      )
    )

    volume_mounts.append(
      kubernetes.client.V1VolumeMount(name="dshm", mount_path="/dev/shm")
    )

    parsed_env = list(map(lambda e: kubernetes.client.V1EnvVar(name=e["name"], value=e["value"]), env or []))


    job = kubernetes.client.V1Job(
      api_version="batch/v1",
      kind="Job",
      metadata=kubernetes.client.V1ObjectMeta(name=name, labels=labels),
      spec=kubernetes.client.V1JobSpec(
        backoff_limit=0,
        ttl_seconds_after_finished=180,
        template=kubernetes.client.V1PodTemplateSpec(
          metadata=kubernetes.client.V1ObjectMeta(labels=labels),
          spec=kubernetes.client.V1PodSpec(
            service_account_name=service_account,
            containers=[
              kubernetes.client.V1Container(
                name="alphafold",
                image=image,
                image_pull_policy="Always",
                command=command,
                args=args,
                env=parsed_env,
                resources=kubernetes.client.V1ResourceRequirements(
                  requests={"cpu": cpu[0], "memory": memory[0]},
                  limits={"cpu": cpu[1], "memory": memory[1]},
                ),
                security_context=kubernetes.client.V1SecurityContext(
                  allow_privilege_escalation=False,
                  capabilities=kubernetes.client.V1Capabilities(
                    drop=["ALL"]
                  ),
                  run_as_group=1000,
                  run_as_user=1000,
                ),
                volume_mounts=volume_mounts,
              )
            ],
            volumes=volumes,
            restart_policy="Never",
            security_context=kubernetes.client.V1PodSecurityContext(
              fs_group_change_policy="OnRootMismatch",
              run_as_non_root=True,
              seccomp_profile=kubernetes.client.V1SeccompProfile(
                type="RuntimeDefault"
              ),
            ),
          )
        )
      ),
    )
    
    if gpu:
      job.spec.template.spec.containers[0].resources.requests["nvidia.com/gpu"] = 1
      job.spec.template.spec.containers[0].resources.limits["nvidia.com/gpu"] = 1
      job.spec.template.spec.affinity = kubernetes.client.V1Affinity(
        node_affinity=kubernetes.client.V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=kubernetes.client.V1NodeSelector(
                node_selector_terms=[
                    kubernetes.client.V1NodeSelectorTerm(
                        match_expressions=[
                            kubernetes.client.V1NodeSelectorRequirement(
                              key="nvidia.com/gpu.memory",
                              operator="Gt",
                              values=[gpu_mem]
                            )
                        ]
                    )
                ]
            )
        )
    )

    self.api.create_namespaced_job(namespace=namespace, body=job)

    return (name, namespace)
