from pathlib import Path
import os
import uuid
import tempfile
import inspect
from functools import wraps
from kubernetes import client, config
import docker
import pathlib
import ast
import textwrap
from kubernetes import client, config, watch
import os
import subprocess
import pickle
import os
import tempfile
import sys

def build_docker_image_with_platform(dockerfile_dir: Path, image_tag, build_args=None):
    build_context_dir = str(dockerfile_dir.parent)

    cmd = [
        "docker", "build",
        "--platform", "linux/amd64",
        "-t", image_tag,
        "--load",
        "-f", str(dockerfile_dir / "Dockerfile"),
        build_context_dir     
    # Specify Dockerfile explicitly
    ]


    if build_args:
        for key, value in build_args.items():
            cmd.append("--build-arg")
            cmd.append(f"{key}={value}")

    print("building2")
    print(" ".join(cmd))

    try:
        # Redirect all output to stderr so it's visible externally
        result = subprocess.run(
            cmd,
        )
    except subprocess.CalledProcessError as e:
        print("STDOUT:", e.stdout.decode())
        print("STDERR:", e.stderr.decode())
        raise

def fetch_pickle(pod_name, namespace, remote_path, local_dir=None):
    """
    Run `kubectl cp pod:/remote_path local_path` to copy a pickle,
    then load and return the object.
    """
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

    # Execute kubectl cp
    subprocess.run(cmd, check=True)

    # Load the pickle file locally
    with open(local_path, "rb") as f:
        obj = pickle.load(f)
    return obj

import os
import pickle
import tempfile
import subprocess

def store_pickle(obj, pod_name, namespace, remote_path, local_dir=None):
    """
    Serialize `obj` to a local pickle file, then use `kubectl cp` to copy
    it to the pod at `remote_path`.
    """
    if local_dir is None:
        local_dir = tempfile.mkdtemp()
    local_path = os.path.join(local_dir, os.path.basename(remote_path))

    # Dump the object to a local pickle file
    with open(local_path, "wb") as f:
        pickle.dump(obj, f)

    cmd = [
        "kubectl", "cp",
        local_path,
        f"{namespace}/{pod_name}:{remote_path}.part",
        "-c",
        "sidecar-keeper"
    ]

    # Execute kubectl cp
    subprocess.run(cmd, check=True)

    cmd = [
        "kubectl", "exec",
        "-n", namespace,
        "-c", "sidecar-keeper",        # container name comes before --
        pod_name,
        "--",
        "mv", f"{remote_path}.part", f"{remote_path}"
    ]

    # Execute kubectl cp
    subprocess.run(cmd, check=True)

    print("args uploaded")

def kuberun(python="3.10", requirements=None, cpu="1", mem="2Gi"):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Load kube config
            config.load_kube_config()

            # Extract function source without decorator, so just pure source
            fn_source = inspect.getsource(fn)
            fn_source = textwrap.dedent(fn_source)
            tree = ast.parse(fn_source)
            
            # drop decorator of running on k8s
            tree = tree.body
            tree[0].name = "fn"
            tree[0].decorator_list = []

            source = ast.unparse(tree)
            
            tag = str(uuid.uuid4())
            import os

            full_name = os.environ.get("IMAGE_REGISTRY", "localhost:5000") + f"/modal_clone:{tag}"

            script_dir = Path(__file__).resolve().parent
            file_path = script_dir / "resources" / "source.py"
            file_path.write_text(source)

            
            print(os.environ.get("DOCKER_DEFAULT_PLATFORM"))
            print(full_name)

            docker_client = docker.from_env()
            # image = docker_client.images.build(
            #     platform="linux/amd64",
            #     buildargs={"requirements": " ".join(requirements)},
            #     path=str(pathlib.Path(__file__).parent.resolve()),
            #     tag=full_name
            # )

            from python_on_whales import docker as dockerv2
            # image = dockerv2.build(context_path=".", 
            #                        file=str(pathlib.Path(__file__).parent.resolve() / "Dockerfile"),
            #                        tags=full_name,
            #                        build_args={"requirements": " ".join(requirements)})

            # print(image)
            
            build_docker_image_with_platform(dockerfile_dir=pathlib.Path(__file__).parent.resolve() / "resources",
                                             image_tag=full_name,
                                             build_args={"requirements": " ".join(requirements)})
            
            # Push the image
            print(f"Pushing image {full_name} to registry...")
            for line in docker_client.images.push(repository=full_name, stream=True, decode=True):
                print(line)
                
            # print(image)

            file_path.unlink()
            
            print(full_name)
            
                
            # push, if not using the local registry

            # # Create temporary files
            # temp_dir = tempfile.mkdtemp()
            # main_path = os.path.join(temp_dir, "main.py")
            # req_path = os.path.join(temp_dir, "requirements.txt")

            # with open(main_path, "w") as f:
            #     f.write(fn_source)
            #     f.write(f"\n\nif __name__ == '__main__':\n    {fn.__name__}()")

            # if requirements:
            #     with open(req_path, "w") as f:
            #         f.write("\n".join(requirements))

            job_name = f"test-job-new-{tag}"


            # Define the shared emptyDir volume
            shared_volume = client.V1Volume(
                name="shared-output",
                empty_dir=client.V1EmptyDirVolumeSource()
            )


            # Main container mounts the shared volume at /app/output
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
                    # limits={"cpu": cpu, "memory": mem},
                    requests={"cpu": cpu, "memory": mem, "ephemeral-storage": "5Gi"}
                )
            )

            # Sidecar container just sleeps, mounts the same volume
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

            # Pod spec with restartPolicy and two containers
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

            # Submit Pod to Kubernetes
            core_v1 = client.CoreV1Api()
            NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")

            core_v1.create_namespaced_pod(namespace=NAMESPACE, body=pod)

            watcher = watch.Watch()
                
            # print(f"[✓] Pod '{job_name}' submitted.")
            # print(f"    Check logs with: kubectl logs {job_name}")
            args_sent = False

            for event in watcher.stream(
                func=core_v1.list_namespaced_pod,
                namespace=NAMESPACE,
                field_selector=f"metadata.name={job_name}",
                timeout_seconds=300,
            ):
                pod = event["object"]
                phase = pod.status.phase
                print(f"Pod phase: {phase}")

                # Find main container status by name
                main_container_status = None
                if pod.status.container_statuses:
                    for cstatus in pod.status.container_statuses:
                        if cstatus.name == tag:  # 'tag' is your main container name
                            main_container_status = cstatus
                            break

                if main_container_status:
                    state = main_container_status.state

                    if state.running and not(args_sent):
                        store_pickle(args[0], job_name, NAMESPACE, remote_path="/app/output/input.pkl")
                        args_sent = True

                    if state.terminated and state.terminated.exit_code == 0:
                        print(f"[✓] Main container '{tag}' completed successfully.")

                        # Now fetch the pickle from the sidecar container (or shared volume)
                        result = fetch_pickle(job_name, NAMESPACE, "/app/output/output.pkl")

                        # Cleanup the pod
                        # Force delete the pod
                        response = core_v1.delete_namespaced_pod(
                            name=job_name,
                            namespace=NAMESPACE,
                            body=client.V1DeleteOptions(
                                grace_period_seconds=0,
                                propagation_policy='Foreground'  # Or 'Background' depending on cleanup preference
                            )
                        )
                        print(f"Force delete initiated for pod '{job_name}': {response.status}")
                        
                        return result
                    elif state.terminated:
                        print(f"[✗] Main container '{tag}' terminated with exit code {state.terminated.exit_code}.")
                        # Optionally handle failure or raise error here

        return wrapper
    return decorator

if __name__ == "__main__":
    from functools import wraps
    import os

    @kuberun(requirements=["transformers", "torch"])
    def test_function(sentence):
        """
        Inside Kubernetes with HF model: accepts a sentence, prints sentiment, returns the result.
        """
        from transformers import pipeline

        print("Running inside container with Hugging Face Transformers!")

        print(f"Received sentence:\n  {sentence}")

        # Load sentiment analysis pipeline using a pretrained model
        classifier = pipeline("sentiment-analysis")
        res = classifier(sentence)[0]

        label = res["label"]
        score = res["score"]

        print(f"Sentiment: {label} (confidence: {score:.3f})")

        return {"label": label, "score": score}

                
    result = test_function("You're a really meh guy... not great at all")

    print(result)