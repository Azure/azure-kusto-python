"""Setup for Azure.Kusto.Data"""

# To use a consistent encoding
import codecs

import re
from os import path

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

PACKAGE_NAME = "azure-kusto-data"

# a-b-c => a/b/c
package_folder_path = PACKAGE_NAME.replace("-", path.sep)
# a-b-c => a.b.c
namespace_name = PACKAGE_NAME.replace("-", ".")

with open(path.join(package_folder_path, "_version.py"), "r") as fd:
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
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
    ],
    namespace_packages=["azure"],
    keywords="kusto wrapper client library",
    packages=find_packages(exclude=["azure", "tests"]),
    install_requires=["adal>=1.0.0", "python-dateutil>=2.8.0", "requests>=2.13.0", "six>=1.10.0"],
    extras_require={"pandas": ["pandas==0.24.1"], ":python_version<'3.0'": ["azure-nspkg"]},
)
