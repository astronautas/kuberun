from pathlib import Path
import os
import uuid
import tempfile
import inspect
from functools import wraps
from kubernetes import client, config, watch
import docker
import pathlib
import ast
import textwrap
import subprocess
import pickle
import sys
from loguru import logger


def build_docker_image_with_platform(dockerfile_dir: Path, image_tag, build_args=None):
    build_context_dir = str(dockerfile_dir.parent)
    cmd = [
        "docker", "build",
        "--platform", "linux/amd64",
        "-t", image_tag,
        "--load",
        "-f", str(dockerfile_dir / "Dockerfile"),
        build_context_dir
    ]
    if build_args:
        for key, value in build_args.items():
            cmd.append("--build-arg")
            cmd.append(f"{key}={value}")

    logger.info(f"Building Docker image: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"STDOUT: {e.stdout.decode() if e.stdout else ''}")
        logger.error(f"STDERR: {e.stderr.decode() if e.stderr else ''}")
        raise


def fetch_pickle(pod_name, namespace, remote_path, local_dir=None):
    if local_dir is None:
        local_dir = tempfile.mkdtemp()
    local_path = os.path.join(local_dir, os.path.basename(remote_path))
    cmd = [
        "kubectl", "cp",
        f"{namespace}/{pod_name}:{remote_path}",
        local_path,
        "-c",
        "sidecar-keeper"
    ]
    subprocess.run(cmd, check=True)
    with open(local_path, "rb") as f:
        obj = pickle.load(f)
    return obj


def store_pickle(obj, pod_name, namespace, remote_path, local_dir=None):
    if local_dir is None:
        local_dir = tempfile.mkdtemp()
    local_path = os.path.join(local_dir, os.path.basename(remote_path))
    with open(local_path, "wb") as f:
        pickle.dump(obj, f)
    cmd = [
        "kubectl", "cp",
        local_path,
        f"{namespace}/{pod_name}:{remote_path}.part",
        "-c",
        "sidecar-keeper"
    ]
    subprocess.run(cmd, check=True)
    cmd = [
        "kubectl", "exec",
        "-n", namespace,
        "-c", "sidecar-keeper",
        pod_name,
        "--",
        "mv", f"{remote_path}.part", f"{remote_path}"
    ]
    subprocess.run(cmd, check=True)
    logger.info("Pickle args uploaded to pod.")


def kuberun(python="3.10", requirements=None, cpu="1", mem="2Gi"):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            config.load_kube_config()

            fn_source = inspect.getsource(fn)
            fn_source = textwrap.dedent(fn_source)
            tree = ast.parse(fn_source)
            tree = tree.body
            tree[0].name = "fn"
            tree[0].decorator_list = []
            source = ast.unparse(tree)

            tag = str(uuid.uuid4())
            full_name = os.environ.get("IMAGE_REGISTRY", "localhost:5000") + f"/modal_clone:{tag}"

            script_dir = Path(__file__).resolve().parent
            file_path = script_dir / "resources" / "source.py"
            file_path.write_text(source)

            logger.info(f"Using Docker platform: {os.environ.get('DOCKER_DEFAULT_PLATFORM')}")
            logger.info(f"Image tag: {full_name}")

            docker_client = docker.from_env()
            build_docker_image_with_platform(
                dockerfile_dir=pathlib.Path(__file__).parent.resolve() / "resources",
                image_tag=full_name,
                build_args={"requirements": " ".join(requirements) if requirements else ""}
            )

            logger.info(f"Pushing image {full_name} to registry...")
            for line in docker_client.images.push(repository=full_name, stream=True, decode=True):
                logger.info(line)

            file_path.unlink()

            job_name = f"test-job-new-{tag}"

            shared_volume = client.V1Volume(
                name="shared-output",
                empty_dir=client.V1EmptyDirVolumeSource()
            )

            main_container = client.V1Container(
                name=tag,
                image=full_name,
                image_pull_policy="IfNotPresent",
                command=["/bin/sh", "-c"],
                args=["python /app/resources/template.py"],
                volume_mounts=[client.V1VolumeMount(
                    name="shared-output",
                    mount_path="/app/output"
                )],
                resources=client.V1ResourceRequirements(
                    requests={"cpu": cpu, "memory": mem, "ephemeral-storage": "5Gi"}
                )
            )

            sidecar_container = client.V1Container(
                name="sidecar-keeper",
                image="busybox",
                command=["/bin/sh", "-c"],
                args=["sleep 3600"],
                volume_mounts=[client.V1VolumeMount(
                    name="shared-output",
                    mount_path="/app/output"
                )]
            )

            pod_spec = client.V1PodSpec(
                restart_policy="Never",
                containers=[main_container, sidecar_container],
                volumes=[shared_volume]
            )

            pod = client.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=client.V1ObjectMeta(name=job_name),
                spec=pod_spec
            )

            core_v1 = client.CoreV1Api()
            NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")
            core_v1.create_namespaced_pod(namespace=NAMESPACE, body=pod)

            watcher = watch.Watch()
            args_sent = False

            for event in watcher.stream(
                func=core_v1.list_namespaced_pod,
                namespace=NAMESPACE,
                field_selector=f"metadata.name={job_name}",
                timeout_seconds=300,
            ):
                pod = event["object"]
                phase = pod.status.phase
                logger.info(f"Pod phase: {phase}")

                main_container_status = None
                if pod.status.container_statuses:
                    for cstatus in pod.status.container_statuses:
                        if cstatus.name == tag:
                            main_container_status = cstatus
                            break

                if main_container_status:
                    state = main_container_status.state

                    if state.running and not args_sent:
                        store_pickle(args[0], job_name, NAMESPACE, remote_path="/app/output/input.pkl")
                        args_sent = True

                    if state.terminated and state.terminated.exit_code == 0:
                        logger.success(f"Main container '{tag}' completed successfully.")
                        result = fetch_pickle(job_name, NAMESPACE, "/app/output/output.pkl")
                        response = core_v1.delete_namespaced_pod(
                            name=job_name,
                            namespace=NAMESPACE,
                            body=client.V1DeleteOptions(
                                grace_period_seconds=0,
                                propagation_policy='Foreground'
                            )
                        )
                        logger.info(f"Force delete initiated for pod '{job_name}': {response.status}")
                        return result

                    elif state.terminated:
                        logger.error(f"Main container '{tag}' terminated with exit code {state.terminated.exit_code}.")
        return wrapper
    return decorator


if __name__ == "__main__":
    @kuberun(requirements=["transformers", "torch"])
    def test_function(sentence):
        from transformers import pipeline
        logger.info("Running inside container with Hugging Face Transformers!")
        logger.info(f"Received sentence: {sentence}")
        classifier = pipeline("sentiment-analysis")
        res = classifier(sentence)[0]
        label = res["label"]
        score = res["score"]
        logger.info(f"Sentiment: {label} (confidence: {score:.3f})")
        return {"label": label, "score": score}

    result = test_function("You're a really meh guy... not great at all")
    logger.info(result)
