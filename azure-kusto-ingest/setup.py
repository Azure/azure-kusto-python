"""Setup for Azure.Kusto.Ingest"""

import re
from os import path
from setuptools import setup, find_packages

PACKAGE_NAME = "azure-kusto-ingest"

# a-b-c => a/b/c
PACKAGE_FOLDER_PATH = PACKAGE_NAME.replace("-", path.sep)
# a-b-c => a.b.c
NAMESPACE_NAME = PACKAGE_NAME.replace("-", ".")

with open(path.join(PACKAGE_FOLDER_PATH, "_version.py"), "r") as fd:
    VERSION = re.search(r'^VERSION\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

if not VERSION:
    raise RuntimeError("Cannot find version information")


setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description="Kusto Ingest Client",
    long_description=open("README.rst", "r").read(),
    license="MIT",
    author="Microsoft Corporation",
    author_email="kustalk@microsoft.com",
    url="https://github.com/Azure/azure-kusto-python",
    namespace_packages=["azure"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
    packages=find_packages(exclude=["azure", "tests"]),
    install_requires=["azure-kusto-data>={}".format(VERSION), "azure-storage-blob==2.1.0", "azure-storage-common==2.1.0", "azure-storage-queue==2.1.0"],
    extras_require={"pandas": ["pandas==0.24.1"], ":python_version<'3.0'": ["azure-nspkg"]},
)
