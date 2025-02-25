# airflow_ray_perjob_operator
This is a simple operator for running a Ray Per job mode on Airflow.

## How to use?
- set up the operator or copy the ray_per_job_operator.py code to your dag file
```shell
pip install -e ./ or pip install https://github.com/zhangkuantian/airflow_ray_perjob_operator.git
```
- copy the ray_yaml/ray_per_job_config.yaml to your dag folder's ray_yaml/ray_per_job_config.yaml


more detail sess: [example](examples/example.py)

