"""Setup for Azure.Kusto.Ingest"""

import re
from os import path
from setuptools import setup, find_packages

try:
    from azure_bdist_wheel import cmdclass
except ImportError:
    from distutils import log as logger

    logger.warn("Wheel is not available, disabling bdist_wheel hook")
    cmdclass = {}

PACKAGE_NAME = "azure-kusto-ingest"

# a-b-c => a/b/c
package_folder_path = PACKAGE_NAME.replace("-", path.sep)
# a-b-c => a.b.c
namespace_name = PACKAGE_NAME.replace("-", ".")

with open(path.join(package_folder_path, "_version.py"), "r") as fd:
    VERSION = re.search(r'^VERSION\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description="Kusto Ingest Client",
    long_description=open("README.rst", "r").read(),
    license="MIT",
    author="Microsoft Corporation",
    author_email="kustalk@microsoft.com",
    url="https://github.com/Azure/azure-kusto-python",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
    ],
    packages=find_packages(),
    install_requires=[
        "azure-kusto-data>={}".format(VERSION),
        "azure-storage-blob>=1.1.0",
        "azure-storage-common>=1.1.0",
        "azure-storage-queue>=1.1.0",
        "six>=1.10.0",
    ],
    extras_require={"pandas": ["pandas>=0.15.0"]},
    cmdclass=cmdclass,
)
