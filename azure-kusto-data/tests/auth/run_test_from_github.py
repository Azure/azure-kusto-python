# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""
This scripts downloads a repo from github and runs a requested test from the repo
It is used to test code that depends on azure resources such as Managed Service Identities
"""

import os
import subprocess
import sys
import zipfile
from urllib import request


def main():
    repo_name = sys.argv[1]
    branch_name = sys.argv[2]
    test_name = sys.argv[3]
    runner = sys.argv[4]
    repo_root = os.path.join(os.getcwd(), "repo")

    try:
        # Create a download directory for the repo
        if not os.path.exists(repo_root):
            os.mkdir(repo_root)

        # Download the code and run the requested test
        load_repo(repo_root, repo_name, branch_name)
        run_test(repo_root, repo_name, branch_name, test_name, runner)
    finally:
        # delete the repo and directory
        if os.path.exists(repo_root):
            clean = "rmdir /s /q " + repo_root
            os.system(clean)


def load_repo(root, name, branch):
    download_url = "https://codeload.github.com/Azure/" + name + "/zip/" + branch
    zip_file = os.path.join(root, "repo.zip")

    request.urlretrieve(download_url, zip_file)
    with zipfile.ZipFile(zip_file) as zip_repo:
        zip_repo.extractall(root)


def run_test(root, name, branch, test, runner):
    path = os.path.join(root, name + "-" + branch.replace("/", "-"))
    path = os.path.join(path, test)
    proc = subprocess.run([runner, path], shell=True, capture_output=True, timeout=5 * 60)
    print("Test Results:")
    print(proc.stdout.decode("ascii"))
    print(proc.stderr.decode("ascii"))
    if proc.returncode != 0:
        raise Exception()


# Run the program
main()
