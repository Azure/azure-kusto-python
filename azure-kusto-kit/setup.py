from setuptools import setup, find_packages

PACKAGE_NAME = "azure-kusto-kit"

setup(
    name=PACKAGE_NAME,
    version="0.1.0",
    description="Kusto Ingestion Tools (Kit)",
    url="https://github.com/Azure/azure-kusto-python",
    author="Microsoft Corporation",
    author_email="kustalk@microsoft.com",
    packages=find_packages(exclude=["azure", "tests"]),
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="kusto wrapper client library",
    entry_points="""
        [console_scripts]
        kit=kit.cli:main
    """,
    install_requires=[
        "azure-kusto-ingest>=0.0.30",
        "azure-kusto-data>=0.0.30",
        "click",
        "maya",
        "pyarrow",
        "pytest",
        "sqlparse"
    ]
)
