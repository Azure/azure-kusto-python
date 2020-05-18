# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import re
from os import path

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

PACKAGE_NAME = "azure-kusto-data"

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
    description="Kusto Data Client",
    long_description=open("README.rst", "r").read(),
    url="https://github.com/Azure/azure-kusto-python",
    author="Microsoft Corporation",
    author_email="kustalk@microsoft.com",
    license="MIT",
    classifiers=[
        # 5 - Production/Stable depends on multi-threading / aio / perf
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
    ],
    namespace_packages=["azure"],
    keywords="kusto wrapper client library",
    packages=find_packages(exclude=["azure", "tests"]),
    install_requires=["adal>=1.0.0", "python-dateutil>=2.8.0", "requests>=2.13.0", "msrestazure>=0.4.14"],
    extras_require={"pandas": ["pandas"]},
)
