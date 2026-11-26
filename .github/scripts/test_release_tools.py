import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, call, patch

import release_tools as release


class ReleaseToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = release.Repository(self.root)
        self.git("init", "--initial-branch=master")
        self.git("config", "user.name", "Release tests")
        self.git("config", "user.email", "release-tests@example.invalid")
        self.github = Mock(spec=release.GitHub)
        self.github.repository = "Azure/azure-kusto-python"
        self.before = self.commit_version("6.0.4")
        self.after = self.commit_version("6.0.5")
        self.github.tag_commit.return_value = self.after
        self.github.pages.return_value = []

    def git(self, *args: str) -> str:
        return release.command(["git", *args], self.root).strip()

    def commit_version(self, version: str) -> str:
        for name, filename in release.PROJECTS.items():
            path = self.root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f'[project]\nname = "{name}"\nversion = "{version}"\n', encoding="utf-8")
        (self.root / "uv.lock").write_text("\n".join(f'[[package]]\nname = "{name}"\nversion = "{version}"\n' for name in release.PROJECTS), encoding="utf-8")
        (self.root / "CHANGELOG.md").write_text(
            f"# Changelog\n\n## [Unreleased]\n\n## [{version}] - 2026-11-24\n\n### Fixed\n- Release fix.\n\n## [5.0.0] - 2025-01-01\n\n- Older notes.\n",
            encoding="utf-8",
        )
        return self.commit()

    def commit(self) -> str:
        self.git("add", ".")
        self.git("commit", "-m", "Update release fixture")
        return self.repo.head()

    def event(self, before: str | None = None, after: str | None = None) -> dict[str, object]:
        return {"ref": "refs/heads/master", "forced": False, "deleted": False, "before": before or self.before, "after": after or self.after}

    def make_distributions(self, metadata_version: str = "6.0.5") -> Path:
        directory = self.root / "dist"
        directory.mkdir(exist_ok=True)
        for package in release.PACKAGES:
            basename = f"{package.replace('-', '_')}-6.0.5"
            metadata = f"Metadata-Version: 2.4\nName: {package}\nVersion: {metadata_version}\n\n".encode()
            with zipfile.ZipFile(directory / f"{basename}-py3-none-any.whl", "w") as archive:
                archive.writestr(f"{basename}.dist-info/METADATA", metadata)
            with tarfile.open(directory / f"{basename}.tar.gz", "w:gz") as archive:
                info = tarfile.TarInfo(f"{basename}/PKG-INFO")
                info.size = len(metadata)
                archive.addfile(info, io.BytesIO(metadata))
        return directory

    def uploaded_asset(self, path: Path) -> dict[str, object]:
        return {"id": 456, "name": path.name, "state": "uploaded", "size": path.stat().st_size, "digest": f"sha256:{release.file_digest(path)}"}

    def mock_release(self, assets: list[dict[str, object]] | None = None, *, draft: bool = False, prerelease: bool = False) -> None:
        existing = {
            "id": 123,
            "tag_name": "v6.0.5",
            "draft": draft,
            "prerelease": prerelease,
            "body": "Maintainer notes",
            "html_url": "https://example.invalid/release",
        }
        self.github.pages.side_effect = [[existing], [] if assets is None else assets]

    def test_release_pr_transition_and_notes(self) -> None:
        planned = release.prepare_push(self.repo, self.event())
        self.assertIsNotNone(planned)
        if planned is not None:
            self.assertEqual((planned.version, planned.sha, planned.notes), ("6.0.5", self.after, "### Fixed\n- Release fix.\n"))

    def test_unchanged_versions_do_not_release(self) -> None:
        (self.root / "pyproject.toml").write_text(
            (self.root / "pyproject.toml").read_text(encoding="utf-8") + 'description = "Dependency-only or tooling edit"\n', encoding="utf-8"
        )
        unchanged = self.commit()
        self.assertIsNone(release.prepare_push(self.repo, self.event(self.after, unchanged)))
        event = {"pull_request": {"base": {"ref": "master", "sha": self.after}}}
        self.assertIsNone(release.validate_pr(self.repo, event, unchanged))
        self.github.assert_not_called()

    def test_pr_cli_validates_metadata_without_mutating_github(self) -> None:
        event = self.root / "event.json"
        event.write_text(json.dumps({"pull_request": {"base": {"ref": "master", "sha": self.before}}}), encoding="utf-8")
        with (
            patch.dict(os.environ, {"GITHUB_EVENT_NAME": "pull_request", "GITHUB_OUTPUT": ""}),
            patch.object(sys, "argv", ["release_tools.py", "validate-pr", "--event", str(event), "--sha", self.after]),
            patch.object(release, "Repository", return_value=self.repo),
            patch.object(release, "GitHub") as github,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(release.main(), 0)
            github.assert_not_called()

    def test_pr_validation_rejects_missing_changelog_before_merge(self) -> None:
        event = {"pull_request": {"base": {"ref": "master", "sha": self.before}}}
        planned = release.validate_pr(self.repo, event, self.after)
        self.assertIsNotNone(planned)
        if planned is not None:
            self.assertEqual(planned.sha, self.after)
        (self.root / "CHANGELOG.md").write_text("## [6.0.4]\n\nOld notes only.\n", encoding="utf-8")
        invalid = self.commit()
        with self.assertRaisesRegex(release.ReleaseError, "changelog section"):
            release.validate_pr(self.repo, event, invalid)
        self.github.request.assert_not_called()

    def test_multi_commit_push_uses_before_snapshot(self) -> None:
        (self.root / "README.md").write_text("Another commit after the bump.\n", encoding="utf-8")
        last = self.commit()
        planned = release.prepare_push(self.repo, self.event(self.before, last))
        self.assertIsNotNone(planned)
        if planned is not None:
            self.assertEqual(planned.sha, last)

    def test_shallow_checkout_fetches_the_previous_snapshot(self) -> None:
        checkout = self.root / "shallow"
        release.command(["git", "clone", "--depth=1", self.root.as_uri(), str(checkout)])
        planned = release.prepare_push(release.Repository(checkout), self.event())
        self.assertIsNotNone(planned)
        if planned is not None:
            self.assertEqual(planned.sha, self.after)

    def test_invalid_push_contexts_fail(self) -> None:
        for overrides in (
            {"ref": "refs/heads/feature"},
            {"forced": True},
            {"deleted": True},
            {"before": "0" * 40},
            {"after": self.before},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(release.ReleaseError):
                release.prepare_push(self.repo, self.event() | overrides)

    def test_inconsistent_release_metadata_fails(self) -> None:
        for filename, original, replacement in (
            ("azure-kusto-data/pyproject.toml", 'version = "6.0.5"', 'version = "6.0.4"'),
            ("uv.lock", 'version = "6.0.5"', 'version = "6.0.4"'),
            ("CHANGELOG.md", "[6.0.5]", "[6.0.6]"),
        ):
            with self.subTest(filename=filename):
                path = self.root / filename
                contents = path.read_text(encoding="utf-8")
                path.write_text(contents.replace(original, replacement, 1), encoding="utf-8")
                invalid = self.commit()
                with self.assertRaises(release.ReleaseError):
                    release.prepare_push(self.repo, self.event(self.before, invalid))
                event = {"pull_request": {"base": {"ref": "master", "sha": self.before}}}
                with self.assertRaises(release.ReleaseError):
                    release.validate_pr(self.repo, event, invalid)
                path.write_text(contents, encoding="utf-8")
                self.commit()

    def test_decreasing_and_prerelease_versions_fail(self) -> None:
        for version in ("6.0.3", "6.0.6rc1", "06.0.6"):
            with self.subTest(version=version):
                invalid = self.commit_version(version)
                with self.assertRaises(release.ReleaseError):
                    release.prepare_push(self.repo, self.event(self.before, invalid))

    def test_publishing_requires_matching_tag_and_commit(self) -> None:
        planned = release.validate_tag(self.repo, self.github, "refs/tags/v6.0.5", self.after)
        self.assertEqual(planned.sha, self.after)
        for ref, sha in (("refs/heads/master", self.after), ("refs/tags/v6.0.4", self.after), ("refs/tags/v6.0.5", self.before)):
            with self.subTest(ref=ref, sha=sha), self.assertRaises(release.ReleaseError):
                release.validate_tag(self.repo, self.github, ref, sha)
        self.github.tag_commit.return_value = self.before
        with self.assertRaises(release.ReleaseError):
            release.validate_tag(self.repo, self.github, "refs/tags/v6.0.5", self.after)

    def test_create_tag_and_explicitly_dispatch(self) -> None:
        self.github.tag_commit.return_value = None
        self.github.request.return_value = {"html_url": "https://github.com/Azure/azure-kusto-python/actions/runs/123"}
        release.tag_and_dispatch(self.repo, self.github, "v6.0.5", self.after)
        self.assertEqual(
            self.github.request.call_args_list,
            [
                call("POST", "git/refs", {"ref": "refs/tags/v6.0.5", "sha": self.after}),
                call("POST", "actions/workflows/release.yml/dispatches", {"ref": "v6.0.5", "return_run_details": True}),
            ],
        )

    def test_successful_empty_dispatch_response_is_not_a_failure(self) -> None:
        self.github.request.return_value = None
        with redirect_stdout(io.StringIO()) as output:
            release.tag_and_dispatch(self.repo, self.github, "v6.0.5", self.after)
        self.github.request.assert_called_once_with("POST", "actions/workflows/release.yml/dispatches", {"ref": "v6.0.5", "return_run_details": True})
        self.assertIn("Dispatched release workflow for v6.0.5", output.getvalue())

    def test_existing_tag_recovers_dispatch_but_never_moves(self) -> None:
        self.github.request.return_value = {"html_url": "https://github.com/Azure/azure-kusto-python/actions/runs/123"}
        release.tag_and_dispatch(self.repo, self.github, "v6.0.5", self.after)
        self.github.request.assert_called_once_with("POST", "actions/workflows/release.yml/dispatches", {"ref": "v6.0.5", "return_run_details": True})
        self.github.reset_mock()
        self.github.tag_commit.return_value = self.before
        with self.assertRaisesRegex(release.ReleaseError, "refusing to move"):
            release.tag_and_dispatch(self.repo, self.github, "v6.0.5", self.after)
        self.github.request.assert_not_called()

    def test_existing_push_or_dispatch_run_is_not_duplicated(self) -> None:
        for event, status, conclusion in (("push", "in_progress", None), ("workflow_dispatch", "completed", "success"), ("push", "completed", "failure")):
            with self.subTest(event=event, status=status):
                self.github.reset_mock()
                self.github.pages.return_value = [
                    {
                        "event": event,
                        "status": status,
                        "conclusion": conclusion,
                        "head_branch": "v6.0.5",
                        "head_sha": self.after,
                        "html_url": "https://example.invalid/run",
                    }
                ]
                if conclusion == "failure":
                    with self.assertRaisesRegex(release.ReleaseError, "retry its failed jobs"):
                        release.tag_and_dispatch(self.repo, self.github, "v6.0.5", self.after)
                else:
                    release.tag_and_dispatch(self.repo, self.github, "v6.0.5", self.after)
                self.github.request.assert_not_called()

    def test_distribution_metadata_must_match_before_upload(self) -> None:
        directory = self.make_distributions()
        self.assertEqual(len(release.distributions(directory, "6.0.5")), 4)
        self.make_distributions(metadata_version="6.0.4")
        with self.assertRaisesRegex(release.ReleaseError, "metadata does not match"):
            release.distributions(directory, "6.0.5")

    def test_existing_release_preserves_notes_and_matching_assets(self) -> None:
        directory = self.make_distributions()
        files = release.distributions(directory, "6.0.5")
        self.mock_release([self.uploaded_asset(files[0])])
        release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.github.request.assert_not_called()
        self.assertEqual(self.github.upload_asset.call_args_list, [call("v6.0.5", path) for path in files[1:]])

    def test_recovered_stable_release_clears_prerelease_for_drafts_and_published_releases(self) -> None:
        directory = self.make_distributions()
        assets = [self.uploaded_asset(path) for path in release.distributions(directory, "6.0.5")]
        for draft in (True, False):
            with self.subTest(draft=draft):
                self.github.reset_mock(side_effect=True)
                self.mock_release(assets, draft=draft, prerelease=True)
                release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
                expected = {"prerelease": False}
                if draft:
                    expected["draft"] = False
                self.github.request.assert_called_once_with("PATCH", "releases/123", expected)
                self.github.upload_asset.assert_not_called()
                self.github.downloaded_digest.assert_not_called()

    def test_matching_draft_on_later_page_resumes_with_its_assets(self) -> None:
        directory = self.make_distributions()
        draft = {"id": 789, "tag_name": "v6.0.5", "draft": True, "body": "Original draft notes", "html_url": "https://example.invalid/draft"}
        assets = [self.uploaded_asset(path) for path in release.distributions(directory, "6.0.5")]
        github = release.GitHub("Azure/azure-kusto-python")
        with (
            patch.object(github, "tag_commit", return_value=self.after),
            patch.object(github, "request", side_effect=[[{"tag_name": f"v0.0.{i}"} for i in range(100)], [draft], assets, {}]) as request,
            patch.object(github, "upload_asset") as upload,
        ):
            release.finalize_release(self.repo, github, "refs/tags/v6.0.5", self.after, directory)
        self.assertEqual(
            request.call_args_list,
            [
                call("GET", "releases?per_page=100&page=1"),
                call("GET", "releases?per_page=100&page=2"),
                call("GET", "releases/789/assets?per_page=100&page=1"),
                call("PATCH", "releases/789", {"draft": False}),
            ],
        )
        upload.assert_not_called()

    def test_ambiguous_matching_releases_fail_without_mutation(self) -> None:
        directory = self.make_distributions()
        self.github.pages.return_value = [{"tag_name": "v6.0.5", "draft": False}, {"tag_name": "v6.0.5", "draft": True}]
        with self.assertRaisesRegex(release.ReleaseError, "Multiple GitHub releases"):
            release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.github.request.assert_not_called()
        self.github.upload_asset.assert_not_called()

    def test_conflicting_asset_is_not_overwritten(self) -> None:
        directory = self.make_distributions()
        first = release.distributions(directory, "6.0.5")[0]
        self.mock_release([self.uploaded_asset(first) | {"digest": "sha256:different"}])
        with self.assertRaisesRegex(release.ReleaseError, "refusing to overwrite"):
            release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.github.upload_asset.assert_not_called()

    def test_zero_byte_starter_asset_is_deleted_and_reuploaded(self) -> None:
        directory = self.make_distributions()
        files = release.distributions(directory, "6.0.5")
        self.mock_release([self.uploaded_asset(files[0]) | {"state": "starter", "size": 0, "digest": None}])
        release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.github.request.assert_called_once_with("DELETE", "releases/assets/456")
        self.assertEqual(self.github.upload_asset.call_args_list, [call("v6.0.5", path) for path in files])
        self.github.downloaded_digest.assert_not_called()

    def test_only_valid_empty_starter_assets_can_be_deleted(self) -> None:
        directory = self.make_distributions()
        first = release.distributions(directory, "6.0.5")[0]
        for overrides in (
            {"state": "starter", "size": 1},
            {"state": "starter", "size": 0, "id": None},
            {"state": "starter", "size": 0, "id": True},
            {"state": "uploaded", "size": 0, "digest": "sha256:different"},
        ):
            with self.subTest(overrides=overrides):
                self.github.reset_mock(side_effect=True)
                self.mock_release([self.uploaded_asset(first) | overrides])
                with self.assertRaises(release.ReleaseError):
                    release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
                self.github.request.assert_not_called()
                self.github.upload_asset.assert_not_called()

    def test_legacy_asset_is_compared_using_downloaded_content(self) -> None:
        directory = self.make_distributions()
        first = release.distributions(directory, "6.0.5")[0]
        self.mock_release([self.uploaded_asset(first) | {"digest": None}])
        self.github.downloaded_digest.return_value = f"sha256:{release.file_digest(first)}"
        release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.github.downloaded_digest.assert_called_once_with("v6.0.5", first.name)
        self.assertNotIn(call("v6.0.5", first), self.github.upload_asset.call_args_list)

    def test_retry_resumes_finalization_without_replacing_uploaded_assets(self) -> None:
        directory = self.make_distributions()
        files = release.distributions(directory, "6.0.5")
        self.mock_release()
        self.github.upload_asset.side_effect = [None, subprocess.CalledProcessError(1, ["gh", "release", "upload"], stderr="Upload interrupted")]
        with self.assertRaises(subprocess.CalledProcessError):
            release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.github.reset_mock(side_effect=True)
        self.mock_release([self.uploaded_asset(files[0])])
        release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.assertEqual(self.github.upload_asset.call_args_list, [call("v6.0.5", path) for path in files[1:]])
        self.github.request.assert_not_called()

    def test_new_release_publishes_only_after_uploads(self) -> None:
        directory = self.make_distributions()
        self.github.pages.side_effect = [[], []]
        self.github.request.side_effect = [{"id": 123, "tag_name": "v6.0.5", "draft": True, "html_url": "https://example.invalid/release"}, {}]
        release.finalize_release(self.repo, self.github, "refs/tags/v6.0.5", self.after, directory)
        self.assertEqual(self.github.upload_asset.call_count, 4)
        self.assertEqual(self.github.request.call_args_list[-1], call("PATCH", "releases/123", {"draft": False}))


class GitHubResponseTests(unittest.TestCase):
    def test_pr_events_cannot_prepare_tag_or_finalize_a_release(self) -> None:
        for args in (
            ["prepare", "--event", "unused.json"],
            ["tag", "--tag", "v6.0.5", "--sha", "a" * 40],
            ["finalize", "--ref", "refs/pull/1/merge", "--sha", "a" * 40, "--dist", "unused"],
        ):
            with (
                self.subTest(action=args[0]),
                patch.dict(
                    os.environ, {"GITHUB_EVENT_NAME": "pull_request", "GITHUB_REF": "refs/pull/1/merge", "GITHUB_REPOSITORY": "Azure/azure-kusto-python"}
                ),
                patch.object(sys, "argv", ["release_tools.py", *args]),
                patch.object(release, "GitHub") as github,
                redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(release.main(), 1)
                self.assertEqual(github.return_value.mock_calls, [])

    def test_only_not_found_can_be_treated_as_missing(self) -> None:
        github = release.GitHub("Azure/azure-kusto-python")
        for code in (404, 403, 429, 500):
            response = subprocess.CompletedProcess(
                ["gh"], 1, f"HTTP/2.0 {code} Error\ncontent-type: application/json\n\n" + json.dumps({"message": "API error"}), ""
            )
            with self.subTest(code=code), patch.object(release.subprocess, "run", return_value=response):
                if code == 404:
                    self.assertIsNone(github.request("GET", "releases/tags/v6.0.5", missing_ok=True))
                else:
                    with self.assertRaisesRegex(release.ReleaseError, f"HTTP {code}"):
                        github.request("GET", "releases/tags/v6.0.5", missing_ok=True)

    def test_annotated_tags_are_resolved(self) -> None:
        github = release.GitHub("Azure/azure-kusto-python")
        with patch.object(
            github,
            "request",
            side_effect=[{"object": {"type": "tag", "sha": "a" * 40}}, {"object": {"type": "commit", "sha": "b" * 40}}],
        ):
            self.assertEqual(github.tag_commit("v6.0.5"), "b" * 40)


if __name__ == "__main__":
    unittest.main()
