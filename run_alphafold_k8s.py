import os
import sys
import pathlib
from alphafold3.data.tools.k8s_utils import KubernetesExecutor
from alphafold3.common import folding_input

def get_arg_value(args, arg):
  for i, a in enumerate(args):
    if a == arg:
      return args[i + 1]
    elif a.startswith(f"{arg}="):
      return a.split("=")[1]

  return None

def remove_args(args, to_remove: list[str]):
  for arg in to_remove:
    if arg in args:
      args.remove(arg)
    else:
      for i, a in enumerate(args):
        if a.startswith(f"{arg}="):
          args.pop(i)
          break

  return args

def assert_env_vars(env_vars: list[str]):
  for env_var in env_vars:
    if os.environ.get(env_var) is None:
      raise AssertionError(f"Environment variable {env_var} is not set.")
    
def get_env_vars(env_vars: list[str]):
  return [{"name": env_var, "value": os.environ.get(env_var, "")} for env_var in env_vars]

def main():
  assert_env_vars(["K8S_JOB_NAME", "K8S_NAMESPACE", "K8S_SERVICE_ACCOUNT", "K8S_IMAGE", "K8S_PVC_MOUNTS", "RUN_K8S_JOBS"])
  job_name = os.environ.get("K8S_JOB_NAME")

  cpu_args = sys.argv[1:]
  input_dir = get_arg_value(cpu_args, "--input_dir")
  json_path = get_arg_value(cpu_args, "--json_path")

  if input_dir is not None:
    fold_inputs = list(folding_input.load_fold_inputs_from_dir(
        pathlib.Path(input_dir)
    ))
  elif json_path is not None:
    fold_inputs = list(folding_input.load_fold_inputs_from_path(
        pathlib.Path(json_path)
    ))
  else:
    raise AssertionError(
        'Exactly one of --json_path or --input_dir must be specified.'
    )
  
  # Assuming only one input/output directory
  cpu_output = fold_inputs[0].sanitised_name()
  print(f"Processing fold input {fold_inputs[0].name}")

  executor = KubernetesExecutor()

  print("Starting CPU job")
  cpu_job = executor.create_job(
    name=f'{job_name}-cpu',
    labels={"job": job_name},
    namespace=os.environ.get('K8S_NAMESPACE', ''),
    service_account=os.environ.get('K8S_SERVICE_ACCOUNT', ''),
    image=os.environ.get('K8S_IMAGE', ''),
    command=['python'],
    args= ["run_alphafold.py"] + cpu_args + ["--norun_inference"],
    env=get_env_vars(["RUN_K8S_JOBS", "K8S_JOB_NAME", "K8S_NAMESPACE", "K8S_SERVICE_ACCOUNT", "K8S_IMAGE", "K8S_PVC_MOUNTS"]),
    pvc_mounts=executor.parse_pvc_mounts(os.environ.get('K8S_PVC_MOUNTS', '')),
    cpu=('2', '4'),
    memory=('2Gi', '4Gi'),
    gpu=False
  )

  try:
    executor.wait_for_job_to_finish(cpu_job, True)
  except Exception as e:
    raise e
  finally:
    executor.delete_job(cpu_job)

  print("CPU job finished") 

  # Update the args so the GPU job starts from the CPU job output
  output_root_dir = get_arg_value(cpu_args, "--output_dir")
  gpu_args = remove_args(cpu_args, ["--json_path", "--input_dir"])
  gpu_args.append(f"--json_path={os.path.join(output_root_dir, cpu_output, cpu_output)}_data.json")
  
  print("Starting GPU job")
  gpu_job = executor.create_job(
    name=f'{job_name}-gpu',
    labels={"job": job_name},
    namespace=os.environ.get('K8S_NAMESPACE', ''),
    service_account=os.environ.get('K8S_SERVICE_ACCOUNT', ''),
    image=os.environ.get('K8S_IMAGE', ''),
    command=['python'],
    args=["run_alphafold.py"] + gpu_args + ["--norun_data_pipeline"],
    env=get_env_vars(["K8S_NAMESPACE", "K8S_SERVICE_ACCOUNT", "K8S_IMAGE", "K8S_PVC_MOUNTS", "XLA_PYTHON_CLIENT_PREALLOCATE", "TF_FORCE_UNIFIED_MEMORY", "XLA_CLIENT_MEM_FRACTION"]),
    pvc_mounts=executor.parse_pvc_mounts(os.environ.get('K8S_PVC_MOUNTS', '')),
    cpu=('8', '16'),
    memory=('64Gi', '128Gi'),
    gpu=True
  )
  
  try:
    executor.wait_for_job_to_finish(gpu_job, True)
  except Exception as e:
    raise e
  finally:
    executor.delete_job(gpu_job)

  print("GPU job finished")

  
if __name__ == '__main__':
  main()