mamba env create -f .devcontainer/environment.yml

mamba activate env_zoo_calrissian

export PYTHONPATH=/data/work/eoepca/eoepca-proc-service-template/tests/water_bodies/

kubectl --namespace zoo port-forward s3-service-7fbbc44d98-wjqsp 9000:9000 9001:9001

Create a file `tests/.env` with:

```
CLIENT_ID = "..."
CLIENT_SECRET = "..."
OIDC_ENDPOINT = "https://auth.demo.eoepca.org/.well-known/openid-configuration"
USER_NAME = "eric"
PASSWORD = "..."
```

Run the tests with:

```
nose2
```
