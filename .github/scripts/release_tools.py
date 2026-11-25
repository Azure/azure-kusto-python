import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path
from typing import TypeGuard
from urllib.parse import urlencode


PROJECTS = {
    "azure-kusto-python": "pyproject.toml",
    "azure-kusto-data": "azure-kusto-data/pyproject.toml",
    "azure-kusto-ingest": "azure-kusto-ingest/pyproject.toml",
    "quick-start": "quick-start/pyproject.toml",
}
PACKAGES = ("azure-kusto-data", "azure-kusto-ingest")


class ReleaseError(RuntimeError):
    pass


def is_object(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)


def object_value(value: object) -> dict[str, object]:
    if not is_object(value):
        raise ReleaseError("Expected a JSON/TOML object.")
    return value


def string_value(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ReleaseError(f"Expected a nonempty string, got {value!r}.")
    return value


def objects(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ReleaseError("Expected an array of objects.")
    return [object_value(item) for item in value]


def command(args: list[str], cwd: Path | None = None, input_text: str | None = None) -> str:
    return subprocess.run(args, cwd=cwd, input=input_text, text=True, encoding="utf-8", capture_output=True, check=True).stdout


def commit_sha(value: object) -> str:
    sha = string_value(value)
    if not re.fullmatch(r"[0-9a-f]{40}", sha) or sha == "0" * 40:
        raise ReleaseError(f"Expected a nonzero commit SHA, got {sha!r}.")
    return sha


def version_numbers(version: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version)
    if match is None:
        raise ReleaseError(f"Only stable major.minor.patch releases are supported: {version!r}.")
    return int(match[1]), int(match[2]), int(match[3])


@dataclass(frozen=True)
class Release:
    version: str
    sha: str
    notes: str

    @property
    def tag(self) -> str:
        return f"v{self.version}"


class Repository:
    def __init__(self, root: Path):
        self.root = root

    def head(self) -> str:
        return commit_sha(command(["git", "rev-parse", "HEAD"], self.root).strip())

    def ensure_commit(self, sha: str) -> None:
        sha = commit_sha(sha)
        kind = command(["git", "cat-file", "--batch-check=%(objecttype)"], self.root, f"{sha}\n").strip()
        if kind == f"{sha} missing":
            command(["git", "fetch", "--no-tags", "--depth=1", "origin", sha], self.root)
        elif kind != "commit":
            raise ReleaseError(f"{sha} is not a commit: {kind}.")

    def read(self, sha: str, path: str) -> str:
        return command(["git", "show", f"{commit_sha(sha)}:{path}"], self.root)

    def versions(self, sha: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for name, path in PROJECTS.items():
            project = object_value(tomllib.loads(self.read(sha, path)).get("project"))
            if project.get("name") != name:
                raise ReleaseError(f"{path} does not describe {name}.")
            result[name] = string_value(project.get("version"))
        return result


def release_notes(changelog: str, version: str) -> str:
    headings = list(re.finditer(r"(?m)^## \[([^\]\r\n]+)\][^\r\n]*\r?\n", changelog))
    matches = [index for index, heading in enumerate(headings) if heading[1] == version]
    if len(matches) != 1:
        raise ReleaseError(f"Expected exactly one changelog section for {version}.")
    index = matches[0]
    end = headings[index + 1].start() if index + 1 < len(headings) else len(changelog)
    notes = changelog[headings[index].end() : end].strip()
    if not notes:
        raise ReleaseError(f"The changelog section for {version} is empty.")
    return notes + "\n"


def snapshot(repo: Repository, sha: str) -> Release:
    versions = repo.versions(sha)
    if len(set(versions.values())) != 1:
        raise ReleaseError(f"Project versions must be synchronized: {versions}.")
    version = versions["azure-kusto-python"]
    version_numbers(version)
    locked = objects(tomllib.loads(repo.read(sha, "uv.lock")).get("package"))
    for name in PROJECTS:
        entries = [entry for entry in locked if entry.get("name") == name]
        if len(entries) != 1 or entries[0].get("version") != version:
            raise ReleaseError(f"uv.lock must contain one workspace record for {name}=={version}.")
    return Release(version, commit_sha(sha), release_notes(repo.read(sha, "CHANGELOG.md"), version))


def prepare_push(repo: Repository, event: dict[str, object]) -> Release | None:
    if event.get("ref") != "refs/heads/master" or event.get("deleted") is not False or event.get("forced") is not False:
        raise ReleaseError("Release preparation requires a non-deleted, non-forced push to master.")
    before, after = commit_sha(event.get("before")), commit_sha(event.get("after"))
    if repo.head() != after:
        raise ReleaseError("The checkout does not match the pushed commit.")
    repo.ensure_commit(before)
    previous, current = repo.versions(before), repo.versions(after)
    if previous == current:
        print("Project versions are unchanged; no tag, dispatch, or publication is needed.")
        return None
    if len(set(previous.values())) != 1:
        raise ReleaseError(f"The previous project versions were not synchronized: {previous}.")
    release = snapshot(repo, after)
    if version_numbers(release.version) <= version_numbers(previous["azure-kusto-python"]):
        raise ReleaseError("The release version must increase.")
    return release


class GitHub:
    def __init__(self, repository: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ReleaseError(f"Invalid GitHub repository: {repository!r}.")
        self.repository = repository

    def request(self, method: str, path: str, data: dict[str, object] | None = None, missing_ok: bool = False) -> object:
        args = [
            "gh",
            "api",
            "--include",
            "--method",
            method,
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            f"repos/{self.repository}/{path}",
        ]
        if data is not None:
            args.extend(["--input", "-"])
        result = subprocess.run(args, input=json.dumps(data) if data is not None else None, text=True, encoding="utf-8", capture_output=True, check=False)
        headers, separator, body = result.stdout.partition("\n\n")
        status = re.match(r"HTTP/[\d.]+ (\d+)", headers)
        if not separator or status is None:
            raise ReleaseError(f"GitHub {method} {path} failed: {result.stderr.strip() or result.stdout.strip()}")
        code = int(status[1])
        if method == "GET" and missing_ok and code == 404:
            return None
        if not 200 <= code < 300 or result.returncode:
            raise ReleaseError(f"GitHub {method} {path} failed with HTTP {code}: {body.strip() or result.stderr.strip()}")
        return json.loads(body) if body.strip() else None

    def tag_commit(self, tag: str) -> str | None:
        ref = self.request("GET", f"git/ref/tags/{tag}", missing_ok=True)
        if ref is None:
            return None
        target = object_value(object_value(ref).get("object"))
        for _ in range(10):
            sha = commit_sha(target.get("sha"))
            if target.get("type") == "commit":
                return sha
            if target.get("type") != "tag":
                raise ReleaseError(f"{tag} does not resolve to a commit.")
            target = object_value(object_value(self.request("GET", f"git/tags/{sha}")).get("object"))
        raise ReleaseError(f"{tag} has too many nested annotated tags.")

    def pages(self, path: str, key: str | None = None) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        page = 1
        while True:
            separator = "&" if "?" in path else "?"
            response = self.request("GET", f"{path}{separator}per_page=100&page={page}")
            items = objects(object_value(response).get(key) if key else response)
            result.extend(items)
            if len(items) < 100:
                return result
            page += 1

    def upload_asset(self, tag: str, path: Path) -> None:
        command(["gh", "release", "upload", tag, str(path), "--repo", self.repository])

    def downloaded_digest(self, tag: str, name: str) -> str:
        with tempfile.TemporaryDirectory() as temporary:
            command(["gh", "release", "download", tag, "--repo", self.repository, "--pattern", name, "--dir", temporary])
            return f"sha256:{file_digest(Path(temporary) / name)}"


def validate_tag(repo: Repository, github: GitHub, ref: str, sha: str) -> Release:
    if not ref.startswith("refs/tags/v"):
        raise ReleaseError("Publishing requires a stable version tag, not a branch.")
    version_numbers(ref.removeprefix("refs/tags/v"))
    sha = commit_sha(sha)
    if repo.head() != sha:
        raise ReleaseError("The checkout does not match the release commit.")
    release = snapshot(repo, sha)
    if ref != f"refs/tags/{release.tag}" or github.tag_commit(release.tag) != sha:
        raise ReleaseError("The version tag, project versions, and release commit must agree.")
    return release


def tag_and_dispatch(repo: Repository, github: GitHub, tag: str, sha: str) -> None:
    version_numbers(tag.removeprefix("v"))
    release = snapshot(repo, commit_sha(sha))
    if tag != release.tag or repo.head() != sha:
        raise ReleaseError("The tag and checkout must match the validated release.")
    existing = github.tag_commit(tag)
    if existing is None:
        github.request("POST", "git/refs", {"ref": f"refs/tags/{tag}", "sha": sha})
        print(f"Created {tag} at {sha}.")
    elif existing != sha:
        raise ReleaseError(f"{tag} already points to {existing}; refusing to move it to {sha}.")
    query = urlencode({"branch": tag, "head_sha": sha})
    runs = [
        run
        for run in github.pages(f"actions/workflows/release.yml/runs?{query}", "workflow_runs")
        if run.get("head_branch") == tag and run.get("head_sha") == sha and run.get("event") in ("push", "workflow_dispatch")
    ]
    for run in runs:
        if run.get("conclusion") == "success" or string_value(run.get("status")) != "completed":
            print(f"A matching release run already exists: {string_value(run.get('html_url'))}")
            return
    if runs:
        raise ReleaseError(f"Inspect {string_value(runs[0].get('html_url'))} and retry its failed jobs; refusing to dispatch another publisher.")
    dispatched = object_value(github.request("POST", "actions/workflows/release.yml/dispatches", {"ref": tag}))
    print(f"Dispatched release workflow: {string_value(dispatched.get('html_url'))}. Dispatch does not imply publication succeeded.")


def distributions(directory: Path, version: str) -> list[Path]:
    version_numbers(version)
    expected = {f"{package.replace('-', '_')}-{version}{suffix}" for package in PACKAGES for suffix in ("-py3-none-any.whl", ".tar.gz")}
    if {path.name for path in directory.iterdir() if path.name != ".gitignore"} != expected:
        raise ReleaseError(f"Expected exactly the SDK wheels and source distributions for {version}.")
    result: list[Path] = []
    for package in PACKAGES:
        basename = f"{package.replace('-', '_')}-{version}"
        wheel, source = directory / f"{basename}-py3-none-any.whl", directory / f"{basename}.tar.gz"
        with zipfile.ZipFile(wheel) as archive:
            entries = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(entries) != 1:
                raise ReleaseError(f"Expected one wheel metadata file in {wheel.name}.")
            wheel_metadata = archive.read(entries[0])
        with tarfile.open(source, "r:gz") as archive:
            entries = [member for member in archive.getmembers() if member.name.endswith("/PKG-INFO") and member.name.count("/") == 1]
            if len(entries) != 1 or (stream := archive.extractfile(entries[0])) is None:
                raise ReleaseError(f"Expected one source metadata file in {source.name}.")
            with stream:
                source_metadata = stream.read()
        for metadata in (wheel_metadata, source_metadata):
            parsed = BytesParser().parsebytes(metadata, headersonly=True)
            if parsed["Name"] != package or parsed["Version"] != version:
                raise ReleaseError(f"Distribution metadata does not match {package}=={version}.")
        result.extend([wheel, source])
    return result


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def finalize_release(repo: Repository, github: GitHub, ref: str, sha: str, directory: Path) -> None:
    release = validate_tag(repo, github, ref, sha)
    files = distributions(directory, release.version)
    response = github.request("GET", f"releases/tags/{release.tag}", missing_ok=True)
    if response is None:
        response = github.request("POST", "releases", {"tag_name": release.tag, "name": release.tag, "body": release.notes, "draft": True, "prerelease": False})
    existing = object_value(response)
    release_id = existing.get("id")
    if (
        not isinstance(release_id, int)
        or isinstance(release_id, bool)
        or release_id <= 0
        or existing.get("tag_name") != release.tag
        or not isinstance(existing.get("draft"), bool)
    ):
        raise ReleaseError("GitHub returned an invalid release.")
    url = string_value(existing.get("html_url"))
    assets = github.pages(f"releases/{release_id}/assets")
    missing: list[Path] = []
    for path in files:
        matches = [asset for asset in assets if asset.get("name") == path.name]
        if len(matches) > 1:
            raise ReleaseError(f"Multiple GitHub assets are named {path.name}.")
        if matches:
            digest = matches[0].get("digest")
            if digest is None:
                digest = github.downloaded_digest(release.tag, path.name)
            if digest != f"sha256:{file_digest(path)}":
                raise ReleaseError(f"Existing release asset {path.name} has different content; refusing to overwrite it.")
            print(f"Keeping identical release asset {path.name}.")
        else:
            missing.append(path)
    for path in missing:
        github.upload_asset(release.tag, path)
        print(f"Uploaded {path.name}.")
    if existing.get("draft") is True:
        github.request("PATCH", f"releases/{release_id}", {"draft": False})
    print(f"GitHub Release finalized: {url}")


def emit_outputs(release: Release | None) -> None:
    values = {"release": "false"} if release is None else {"release": "true", "version": release.version, "tag": release.tag, "sha": release.sha}
    print(json.dumps(values))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a", encoding="utf-8") as stream:
            stream.writelines(f"{key}={value}\n" for key, value in values.items())


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="action", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--event", type=Path, required=True)
    tag = commands.add_parser("tag")
    tag.add_argument("--tag", required=True)
    tag.add_argument("--sha", required=True)
    for name in ("validate", "finalize"):
        tagged = commands.add_parser(name)
        tagged.add_argument("--ref", required=True)
        tagged.add_argument("--sha", required=True)
        if name == "finalize":
            tagged.add_argument("--dist", type=Path, required=True)
    verify = commands.add_parser("verify-dist")
    verify.add_argument("--dist", type=Path, required=True)
    verify.add_argument("--version", required=True)
    args = parser.parse_args()
    try:
        repo = Repository(Path.cwd())
        if args.action == "prepare":
            if os.environ.get("GITHUB_EVENT_NAME") != "push":
                raise ReleaseError("Only a push event can prepare a release.")
            emit_outputs(prepare_push(repo, object_value(json.loads(args.event.read_text(encoding="utf-8")))))
        elif args.action == "verify-dist":
            distributions(args.dist, args.version)
        else:
            github = GitHub(string_value(os.environ.get("GITHUB_REPOSITORY")))
            if args.action == "tag":
                if os.environ.get("GITHUB_EVENT_NAME") != "push" or os.environ.get("GITHUB_REF") != "refs/heads/master":
                    raise ReleaseError("Only a master push can create and dispatch a release tag.")
                tag_and_dispatch(repo, github, args.tag, args.sha)
            else:
                if os.environ.get("GITHUB_EVENT_NAME") not in ("push", "workflow_dispatch"):
                    raise ReleaseError("Only a tag push or tagged dispatch can publish a release.")
                if args.action == "validate":
                    emit_outputs(validate_tag(repo, github, args.ref, args.sha))
                else:
                    finalize_release(repo, github, args.ref, args.sha, args.dist)
        return 0
    except (ReleaseError, OSError, subprocess.CalledProcessError, tomllib.TOMLDecodeError, json.JSONDecodeError, tarfile.TarError, zipfile.BadZipFile) as error:
        detail = error.stderr if isinstance(error, subprocess.CalledProcessError) and error.stderr else str(error)
        print(f"::error::{detail}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
