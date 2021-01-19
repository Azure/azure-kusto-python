import argparse
import os
from subprocess import check_call
from pathlib import Path


try:
    from packaging.version import parse as Version, InvalidVersion
except ImportError:  # Should not happen, but at worst in most case this is the same
    from pip._vendor.packaging.version import parse as Version, InvalidVersion

DEFAULT_DESTINATION_FOLDER = os.path.join("..", "dist")
package_list = ["azure-kusto-data", "azure-kusto-ingest", "azure-kusto-logging"]


def create_package(name, dest_folder=DEFAULT_DESTINATION_FOLDER):
    absdirpath = os.path.abspath(name)
    check_call(["python", "setup.py", "bdist_wheel", "-d", dest_folder], cwd=absdirpath)
    check_call(["python", "setup.py", "sdist", "-d", dest_folder], cwd=absdirpath)


def travis_build_package():
    """Assumed called on Travis, to prepare a package to be deployed
    This method prints on stdout for Travis.
    Return is obj to pass to sys.exit() directly
    """
    travis_tag = os.environ.get("TRAVIS_TAG")
    if not travis_tag:
        print("TRAVIS_TAG environment variable is not present")
        return "TRAVIS_TAG environment variable is not present"

    try:
        version = Version(travis_tag)
    except InvalidVersion:
        failure = "Version must be a valid PEP440 version (version is: {})".format(version)
        print(failure)
        return failure

    abs_dist_path = Path(os.environ["TRAVIS_BUILD_DIR"], "dist")
    [create_package(package, str(abs_dist_path)) for package in package_list]

    print("Produced:\n{}".format(list(abs_dist_path.glob("*"))))

    pattern = "*{}*".format(version)
    packages = list(abs_dist_path.glob(pattern))
    if not packages:
        return "Package version does not match tag {}, abort".format(version)
    pypi_server = os.environ.get("PYPI_SERVER", "default PyPI server")
    print("Package created as expected and will be pushed to {}".format(pypi_server))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Azure package.")
    parser.add_argument("name", help="The package name")
    parser.add_argument("--dest", "-d", default=DEFAULT_DESTINATION_FOLDER, help="Destination folder. Relative to the package dir. [default: %(default)s]")

    args = parser.parse_args()
    if args.name == "all":
        for package in package_list:
            create_package(package, args.dest)
    else:
        create_package(args.name, args.dest)
