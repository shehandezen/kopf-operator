from setuptools import setup, find_packages

setup(
    name="kopf-operator-framework",
    version="0.1.0",
    author="Savindu Shehan",
    description="Reusable advanced Kopf-based Kubernetes operator framework",
    packages=find_packages(),
    install_requires=[
        "kopf>=1.36.2",
        "kubernetes>=26.1.0",
    ],
    python_requires=">=3.8",
    include_package_data=True,
)
