""" Setup for Azure.Kusto.Ingest
"""

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
package_folder_path = PACKAGE_NAME.replace('-', path.sep)
# a-b-c => a.b.c
namespace_name = PACKAGE_NAME.replace('-', '.')

with open(path.join(package_folder_path, 'version.py'), 'r') as fd:
    VERSION = re.search(r'^VERSION\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description='Kusto Ingest Client',
    long_description=open('README.rst', 'r').read(),
    license='MIT',
    author='Microsoft Corporation',
    author_email='kustalk@microsoft.com',
    url='https://kusto.azurewebsites.net/docs/api/kusto_python_client_library.html',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
    ],
    packages=find_packages(),
    install_requires=[
        'adal>=0.4.5',
        'appdirs>=1.4.3',
        'asn1crypto>=0.21.1',
        'azure-kusto-data>=0.0.4',
        'azure-storage-blob>=1.1.0',
        'azure-storage-common>=1.1.0',
        'azure-storage-queue>=1.1.0',
        'azure-nspkg>=1.0.0',
        'cffi>=1.9.1',
        'cryptography>=1.8.1',
        'idna>=2.5',
        'packaging>=16.8',
        'pycparser>=2.17',
        'PyJWT>=1.4.2',
        'pyparsing>=2.2.0',
        'python-dateutil>=2.6.0',
        'requests>=2.13.0',
        'six>=1.10.0',
    ],
    cmdclass=cmdclass,
)
