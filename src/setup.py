from setuptools import setup, find_packages

setup(
    name='master-thesis',
    version='1.0',
    packages=find_packages(),
    package_data={'configs': ['tranco_W9JG9.csv']},
    include_package_data=True
)
