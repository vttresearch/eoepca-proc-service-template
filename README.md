mamba env create -f .devcontainer/environment.yml

mamba activate env_zoo_calrissian

export PYTHONPATH=/data/work/eoepca/eoepca-proc-service-template/tests/water_bodies/

kubectl --namespace zoo port-forward s3-service-7fbbc44d98-wjqsp 9000:9000 9001:9001

nose2