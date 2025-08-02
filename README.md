# run_on_k8s
I love modal.com. Running Python functions in the cloud with just a simple decorator is a brilliant idea.

Unfortunately, I never managed to convince infra teams to switch to modal's managed cloud. Infra teams seem to use K8s...

How about we do this then?

```python
@run_on_k8s(requirements=["pandas", "numpy"], python_version="3.13")
def my_fancy_function(size: int):
  import pandas as pd

  df = pd.DataFrame(np.random.randint(0, size, (10**6, 500)))
  res = df.groupby([df[i] % 100 for i in range(5)]).apply(lambda x: x.sum().rolling(5).mean())

  return res

my_fancy_function(1_000_000) # will run on k8s
my_fancy_function(1_000_000, local=True) # will run locally
```

Infra team happy, and DS happy. Stay tuned.

- [x] Run a single function with no dependencies on k8s default context;
- [x] Run a function with dependencies;
- [x] Arguments pass-in and return
- [+-] Lifecycle (logging back to client, on k8s error exit, etc);
- [ ] Configuration - context, registry, etc
- [ ] Access control - run in specific namespace?
- [ ] Offload image building to k8s itself
- [ ] Make sure context is correctly set