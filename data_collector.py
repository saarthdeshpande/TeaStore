import argparse
import logging
import subprocess
import threading
import time
import shlex

from utils import interval_string_to_seconds

import yaml

logging.basicConfig(level=logging.INFO)

# TODO: dynamically get microservice names
microservices = []

hpa_config_file = "hpa_config.yaml"
locustfile_path = "./locustfile.py"
locust_venv = "./venv/bin" #~/CLionProjects/microservices-demo/src/loadgenerator/venv/bin"
frontend_external_ip = "128.110.96.91:30080/tools.descartes.teastore.webui/"

class LiteralDumper(yaml.SafeDumper):
    pass

def str_presenter(dumper, data):
    # Use block style for multi-line strings
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

LiteralDumper.add_representer(str, str_presenter)

# Optional: keep key order and avoid long line wrapping
LiteralDumper.ignore_aliases = lambda *args: True

yaml_width = 4096

def create_hpa_yaml(args):
    global microservices
    DEF_all = {"req": {"cpu": "500m", "memory": "512Mi"}, "lim": {"cpu": "1000m", "memory": "1Gi"}}
    # Note: teastore-db is MySQL, similar to mongodb, often excluded from HPA or given special limits,
    # but we'll use DEF_all as a default and keep it simple.
    # DEF_db = {"req": {"cpu": "2000m", "memory": "512Mi"}, "lim": {"cpu": "2000m", "memory": "512Mi"}}

    # Build metrics list from args
    metrics = []
    if getattr(args, "cpu", False):
        metrics.append({
            "type": "Resource",
            "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}}
        })
    if getattr(args, "memory", False):
        metrics.append({
            "type": "Resource",
            "resource": {"name": "memory", "target": {"type": "Utilization", "averageUtilization": 80}}
        })

    all_configs = []

    # 📝 Minimal change: Target only the manifest.yaml file
    fn = "manifest.yaml"
    try:
        with open(fn, "r") as f:
            docs = list(yaml.safe_load_all(f))
    except FileNotFoundError:
        print(f"Error: {fn} not found.")
        return

    DEF = DEF_all  # Using DEF_all for simplicity as per original function structure

    # Augment Deployment docs with default resources if missing
    for d in docs:
        if isinstance(d, dict) and d.get("kind") == "Deployment":
            pod_spec = (((d.get("spec") or {}).get("template") or {}).get("spec") or {})
            for k in ("containers", "initContainers"):
                for c in (pod_spec.get(k, []) or []):
                    r = c.setdefault("resources", {})
                    rq, lm = r.setdefault("requests", {}), r.setdefault("limits", {})
                    rq.setdefault("cpu", DEF["req"]["cpu"])
                    rq.setdefault("memory", DEF["req"]["memory"])
                    lm.setdefault("cpu", DEF["lim"]["cpu"])
                    lm.setdefault("memory", DEF["lim"]["memory"])

    # Add an HPA for Deployments when metrics requested
    if metrics:
        new_docs = []
        for d in docs:
            if isinstance(d, dict) and d.get("kind") == "Deployment":
                md = d.get("metadata", {}) or {}
                # 📌 Use the deployment name directly
                name = md.get("name")

                # Optionally, exclude the database from HPA (like 'mongodb' exclusion)
                # if name == "teastore-db":
                #     continue

                if name:
                    microservices.append(name)
                    pMin, pMax = 1, 20
                    new_docs.append({  # Append to new_docs to avoid modifying docs while iterating
                        "apiVersion": "autoscaling/v2",
                        "kind": "HorizontalPodAutoscaler",
                        "metadata": {"name": f"{name}"},  # Use a distinct HPA name
                        "spec": {
                            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": name},
                            "minReplicas": pMin,
                            "maxReplicas": pMax,
                            "metrics": metrics,
                            # "behavior": {
                            #     "scaleUp": {
                            #         "stabilizationWindowSeconds": 300,
                            #         "policies": [
                            #             {
                            #                 "type": "Percent",
                            #                 "value": 100,
                            #                 "periodSeconds": 60,
                            #             }
                            #         ],
                            #     },
                            #     "scaleDown": {
                            #         "stabilizationWindowSeconds": 300,
                            #         "policies": [
                            #             {
                            #                 "type": "Percent",
                            #                 "value": 50,
                            #                 "periodSeconds": 60,
                            #             }
                            #         ],
                            #     },
                            # },
                        }
                    })
        docs.extend(new_docs)  # Add all new HPAs to the documents list

    all_configs.extend(docs)

    with open(hpa_config_file, "w") as f:
        yaml.dump_all(all_configs, f, default_flow_style=False, Dumper=LiteralDumper, width=yaml_width)

def record_hpa_numbers(microservice, metric, duration):
    cmd = f"kubectl get hpa {microservice}"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    start_time = time.time()
    duration_in_seconds = interval_string_to_seconds(duration)

    try:
        with open(f"{metric}/{microservice}.txt", "w") as hpa_output_file:
            while time.time() - start_time < duration_in_seconds:
                output = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if output.stdout:
                    hpa_output_file.write(output.stdout.strip().split("\n")[1])
                    hpa_output_file.write("\n")
                    hpa_output_file.flush()  # Flush after each write
                time.sleep(15)
                # if process.poll() is not None:
                #     break
        logging.info(f"Completed HPA monitoring for {microservice}")
    except Exception as e:
        logging.error(f"Error monitoring HPA for {microservice}: {str(e)}")
    finally:
        process.terminate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cpu", default=False, action='store_true')
    parser.add_argument("-m", "--memory", default=False, action='store_true')
    parser.add_argument("-r", "--realtime", default=False, action='store_true')
    parser.add_argument("-t", "--time", default="10m")

    args = parser.parse_args()
    create_hpa_yaml(args)
    # exit(-1)
    metric = ""
    if args.cpu is True:
        metric += "cpu_"
    if args.memory is True:
        metric += "memory_"

    if not metric:
        logging.warning(f"No metrics specified in args.")
        exit(-1)

    hpaApplyCmd = f"kubectl apply -f {hpa_config_file}"
    hpaApplyProcess = subprocess.Popen(hpaApplyCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    print("Applied app config. Sleeping for 120s while resources provisioned.")
    time.sleep(120)
    print("Running locust workload.")

    print("Collecting HPA data.")

    threads = []
    for hpa in microservices:
        thread = threading.Thread(target=record_hpa_numbers, args=(hpa, metric, args.time))
        # thread.start()
        threads.append(thread)

    locustProcess = None
    flags = f"--headless -u 100 -r 1 -t {args.time} -d {metric}"
    if args.realtime is True:
        flags += " --realtime"
    try:

        locustCmd = f"-f {locustfile_path} --host=http://{frontend_external_ip} {flags}"
        logging.info("Applying locust.")
        command = [locust_venv + "/python", locust_venv + "/locust"] + shlex.split(locustCmd)
        locustProcess = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logging.info("Locust process completed. Sleeping until load builds up.")
        # error_output = locustProcess.stderr.read()
        # if error_output:
        #     logging.error(f"Initial Locust error: {error_output}")
        for thread in threads:
            thread.start()
        stdout, stderr = locustProcess.communicate(timeout=interval_string_to_seconds(args.time) + 90)
    except subprocess.TimeoutExpired:
        logging.error("Locust process timed out")
    finally:
        # Wait for all threads to complete
        for i, thread in enumerate(threads):
            logging.info(f"Waiting for thread {i + 1}/{len(threads)} to complete")
            thread.join()
            logging.info(f"Thread {i + 1}/{len(threads)} completed")

        logging.info("Deleting app config.")
        hpaDeleteCmd = f"kubectl delete -f {hpa_config_file}"
        hpaDeleteProcess = subprocess.Popen(hpaDeleteCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stdout, stderr = hpaDeleteProcess.communicate()

        if hpaDeleteProcess.returncode == 0:
            logging.info("HPA config deleted successfully")
        else:
            logging.error(f"Error deleting HPA config: {stderr}")

