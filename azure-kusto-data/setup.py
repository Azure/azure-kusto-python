"""Setup for Azure.Kusto.Data"""

# To use a consistent encoding
import codecs

import re
from os import path

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

try:
    from azure_bdist_wheel import cmdclass
except ImportError:
    from distutils import log as logger

    logger.warn("Wheel is not available, disabling bdist_wheel hook")
    cmdclass = {}

PACKAGE_NAME = "azure-kusto-data"

# a-b-c => a/b/c
package_folder_path = PACKAGE_NAME.replace("-", path.sep)
# a-b-c => a.b.c
namespace_name = PACKAGE_NAME.replace("-", ".")

with open(path.join(package_folder_path, "_version.py"), "r") as fd:
    VERSION = re.search(r'^VERSION\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

CURRENT_PATH = path.abspath(path.dirname(__file__))
with codecs.open(path.join(CURRENT_PATH, "README.rst"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

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
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="kusto wrapper client library",
    packages=find_packages(),
    install_requires=["adal>=1.0.0", "azure-nspkg>=2.0.0", "python-dateutil>=2.6.0", "requests>=2.13.0", "six>=1.10.0"],
    extras_require={"pandas": ["pandas>=0.15.0"]},
    cmdclass=cmdclass,
)
