import logging
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, Literal, Tuple
from zipfile import ZipFile


def replace_content(ext: Literal["whl", "tar.gz"], path: Path, contents: Dict[str, Tuple[re.Pattern, str]]):
    with tempfile.TemporaryDirectory() as temp_dir:
        with ZipFile(path, "r") if ext == "whl" else tarfile.open(path, "r:gz") as read:
            temp_path = Path(temp_dir)
            read.extractall(temp_path)

        for name, (regex, sub) in contents.items():
            for file in temp_path.glob(f"**/{name}"):
                contents = file.read_text()
                contents = regex.sub(sub, contents)
                file.write_text(contents)
                logger.info(f"Replaced content in {file}")

        with ZipFile(path, "w") if ext == "whl" else tarfile.open(path, "w:gz") as write:
            for file in temp_path.glob("**/*"):
                if not file.is_file():
                    continue
                if ext == "whl":
                    write.write(file, file.relative_to(temp_path))
                else:
                    write.add(file, file.relative_to(temp_path))


# Requires-Dist: azure-kusto-data @ file:///C:/Users/asafmahlev/repos/azure-kusto-python/azure-kusto-data
REGEX_DIST = re.compile(r"Requires-Dist: (?P<name>.*?) @.*")
# 'azure-kusto-data @ '
#  'file:///C:/Users/asafmahlev/repos/azure-kusto-python/azure-kusto-data',
REGEX_SETUP_PY = re.compile(r"'(?P<name>.*?)\s*@.*?(?P=name)',", re.DOTALL)
# azure-kusto-data = { path = "../azure-kusto-data", develop = true }
REGEX_PYPROJECT_TOML = re.compile(r"^(?P<name>.*) = \{ path =.*$", re.MULTILINE)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def main():
    # run poetry version -s
    VERSION = subprocess.check_output(["poetry", "version", "-s"]).decode().strip()
    VERSION_MINOR = ".".join(VERSION.split(".")[:2])
    logger.info(f"Version: {VERSION}, Version Minor: {VERSION_MINOR}")

    logger.info("Updating wheel metadata")
    for p in Path(".").glob("*/dist/*.whl"):
        logger.info(f"Updating {p}")
        replace_content(
            "whl",
            p,
            {
                "METADATA": (REGEX_DIST, rf"Requires-Dist: \\g<name> (~={VERSION_MINOR}, >={VERSION})"),
            },
        )
        logger.info(f"Updated {p}")

    logger.info("Updating tar.gz metadata")
    for p in Path(".").glob("*/dist/*.tar.gz"):
        logger.info(f"Updating {p}")
        replace_content(
            "tar.gz",
            p,
            {
                "PKG-INFO": (REGEX_DIST, rf"Requires-Dist: \g<name> (~={VERSION_MINOR}, >={VERSION})"),
                "setup.py": (REGEX_SETUP_PY, rf"'\g<name>~={VERSION_MINOR},>={VERSION}'"),
                "pyproject.toml": (REGEX_PYPROJECT_TOML, f'\\g<name> = "~{VERSION}"'),
            },
        )
        logger.info(f"Updated {p}")
