from pytest_kubernetes.providers.base import AClusterManager
import subprocess
import pytest
import uuid
import time
from run_on_k8s.main import kuberun
import pandas as pd
from unittest import mock
import os

@pytest.fixture
def tmp_namespace():
    ns = f"test-ns-{uuid.uuid4().hex[:6]}"
    subprocess.check_call(["kubectl", "create", "namespace", ns])
    yield ns
    subprocess.call(["kubectl", "delete", "namespace", ns, "--grace-period=0", "--force"])

def test_k8s(tmp_namespace):
    # Create minimal fake pod YAML or inline spec
    pod_yaml = f"""
apiVersion: v1
kind: Pod
metadata:
  name: fake-pod
  namespace: {tmp_namespace}
spec:
  containers:
  - name: pause
    image: k8s.gcr.io/pause:3.8
    command: ['sleep', '3600']
"""
    proc = subprocess.Popen(
        ["kubectl", "apply", "-f", "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True  # or text=True on newer Python
    )
    stdout, stderr = proc.communicate(input=pod_yaml)
    if proc.returncode != 0:
        raise RuntimeError(f"kubectl failed: {stderr}")
    
def test_run_on_k8s__happy_path(tmp_namespace):
    with mock.patch.dict(os.environ, {'K8S_NAMESPACE': tmp_namespace}):
        # bruh, typing does not work
        # kw args don't work too
        
        # also always pushes to hardcoded repository bruh
        @kuberun(requirements=["pandas"], cpu="0.1", mem="0.5Gi")
        def test_remote_transformation(input_df):
            import pandas as pd
            input_df = input_df.copy()
            input_df["prediction"] = input_df["feature"] * 2

            return input_df
        
        result_df = test_remote_transformation(pd.DataFrame({"feature": [0.1, 0.5]}))

        assert result_df["prediction"].tolist() == [0.2, 1.0]