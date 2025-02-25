# coding:utf-8
from pathlib import Path
from setuptools import setup, find_packages


def read_long_description():
    try:
        return Path("README.md").read_text(encoding="utf-8")
    except FileNotFoundError:
        return "A description of MiniRAG is currently unavailable."


def read_requirements():
    deps = []
    try:
        with open("./requirements.txt") as f:
            deps = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(
            "Warning: 'requirements.txt' not found. No dependencies will be installed."
        )
    return deps


long_description = read_long_description()
requirements = read_requirements()

setup(name='airflow_ray_perjob_operator',
      version='v0.0.1',
      description='This is a setup for airflow_ray_perjob_operator',
      author='some',
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=requirements,
      include_package_data=True,
      author_email='some@app.com',
      url='https://github.com/zhangkuantian/airflow_ray_perjob_operator.git',
      packages=find_packages())
