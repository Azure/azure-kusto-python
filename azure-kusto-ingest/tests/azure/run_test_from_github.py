# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import subprocess
import sys
import zipfile
from urllib import request


def main():
    repo_name = sys.argv[1]
    branch_name = sys.argv[2]
    test_name = sys.argv[3]
    repo_root = os.path.join(os.getcwd(), "root")

    try:
        load_repo(repo_root, repo_name, branch_name)
        run_test(repo_root, repo_name, branch_name, test_name)
    finally:
        clean = "rmdir /s /q " + repo_root
        print(clean)
        os.system(clean)


def load_repo(root, name, branch):
    download_url = "https://codeload.github.com/Azure/" + name + "/zip/" + branch
    zip_file = os.path.join(root, "repo.zip")
    os.mkdir(root)
    request.urlretrieve(download_url, zip_file)
    with zipfile.ZipFile(zip_file) as zip_repo:
        zip_repo.extractall(root)


def run_test(root, name, branch, test):
    path = os.path.join(root, name + "-" + branch)
    path = os.path.join(path, test)
    proc = subprocess.run(path, shell=True, capture_output=True, timeout=5*60)
    print("tests completed")
    print(proc.stdout)
    print(proc.stderr)
    if proc.returncode != 0:
        raise Exception()

# Run the program
main()
