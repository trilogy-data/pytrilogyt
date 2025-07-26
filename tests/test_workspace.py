import tempfile
from pathlib import Path

import pytest

from trilogyt.io import BaseWorkspace, FileWorkspace, MemoryWorkspace


class TestBaseWorkspace:
    def test_init_generates_id(self):
        workspace = BaseWorkspace()
        assert isinstance(workspace.id, int)
        assert 0 <= workspace.id <= 1000000

    def test_get_files_not_implemented(self):
        workspace = BaseWorkspace()
        with pytest.raises(NotImplementedError):
            workspace.get_files()

    def test_get_file_not_implemented(self):
        workspace = BaseWorkspace()
        with pytest.raises(NotImplementedError):
            workspace.get_file(Path("test.txt"))

    def test_file_exists_not_implemented(self):
        workspace = BaseWorkspace()
        with pytest.raises(NotImplementedError):
            workspace.file_exists(Path("test.txt"))

    def test_get_environment_not_implemented(self):
        workspace = BaseWorkspace()
        with pytest.raises(NotImplementedError):
            workspace.get_environment()

    def test_write_file_not_implemented(self):
        workspace = BaseWorkspace()
        with pytest.raises(NotImplementedError):
            workspace.write_file(Path("test.txt"), "content")

    def test_wipe_not_implemented(self):
        workspace = BaseWorkspace()
        with pytest.raises(NotImplementedError):
            workspace.wipe()


class TestFileWorkspace:
    def test_init(self):
        working_path = Path("/tmp")
        paths = [Path("file1.txt"), Path("file2.txt")]
        workspace = FileWorkspace(working_path, paths)
        assert workspace.working_path == working_path
        assert workspace.paths == paths
        assert isinstance(workspace.id, int)

    def test_file_exists_true(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            working_path = Path(tmp_dir)
            test_file = working_path / "test.txt"
            test_file.write_text("content")

            workspace = FileWorkspace(working_path, [])
            assert workspace.file_exists(Path("test.txt"))

    def test_file_exists_false(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            working_path = Path(tmp_dir)
            workspace = FileWorkspace(working_path, [])
            assert not workspace.file_exists(Path("nonexistent.txt"))

    def test_get_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            working_path = Path(tmp_dir)
            test_file = working_path / "test.txt"
            test_content = "test content"
            test_file.write_text(test_content)

            workspace = FileWorkspace(working_path, [])
            assert workspace.get_file(Path("test.txt")) == test_content

    def test_get_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            working_path = Path(tmp_dir)
            file1 = working_path / "file1.txt"
            file2 = working_path / "file2.txt"
            file1.write_text("content1")
            file2.write_text("content2")

            workspace = FileWorkspace(working_path, [file1, file2])
            files = workspace.get_files()
            assert files[file1] == "content1"
            assert files[file2] == "content2"

    def test_get_environment(self):
        working_path = Path("/tmp")
        workspace = FileWorkspace(working_path, [])

        env = workspace.get_environment()

        assert env.working_path == working_path

    def test_write_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            working_path = Path(tmp_dir)
            test_file = working_path / "test.txt"
            test_content = "test content"

            workspace = FileWorkspace(working_path, [])
            workspace.write_file(test_file, test_content)

            assert test_file.read_text() == test_content
            assert test_file in workspace.paths

    def test_wipe_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            working_path = Path(tmp_dir)
            test_file = working_path / "test.txt"
            test_file.write_text("content")
            test_dir = working_path / "subdir"
            test_dir.mkdir()

            workspace = FileWorkspace(working_path, [])
            workspace.wipe()

            assert not test_file.exists()
            assert not test_dir.exists()

    def test_wipe_directory_nonexistent(self):
        nonexistent_path = Path("/tmp/nonexistent")
        workspace = FileWorkspace(nonexistent_path, [])
        workspace.wipe()


class TestMemoryWorkspace:
    def test_init(self):
        workspace = MemoryWorkspace()
        assert workspace.files == {}
        assert isinstance(workspace.id, int)

    def test_repr(self):
        workspace = MemoryWorkspace()
        assert repr(workspace) == "MemoryWorkspace()"

    def test_file_exists_true(self):
        workspace = MemoryWorkspace()
        workspace.files["test.txt"] = "content"
        assert workspace.file_exists(Path("test.txt"))

    def test_file_exists_false(self):
        workspace = MemoryWorkspace()
        assert not workspace.file_exists(Path("test.txt"))

    def test_get_files(self):
        workspace = MemoryWorkspace()
        workspace.files["file1.txt"] = "content1"
        workspace.files["file2.txt"] = "content2"

        files = workspace.get_files()
        assert files[Path("file1.txt")] == "content1"
        assert files[Path("file2.txt")] == "content2"

    def test_get_file(self):
        workspace = MemoryWorkspace()
        workspace.files["test.txt"] = "test content"
        assert workspace.get_file(Path("test.txt")) == "test content"

    def test_get_environment(self):
        workspace = MemoryWorkspace()
        workspace.files["test.txt"] = "content"

        env = workspace.get_environment()

        assert env.working_path == Path.cwd()

    def test_write_file(self):
        workspace = MemoryWorkspace()
        path = Path("test.txt")
        content = "test content"

        result = workspace.write_file(path, content)

        assert workspace.files["test.txt"] == content
        assert result == path

    def test_wipe(self):
        workspace = MemoryWorkspace()
        workspace.files["test.txt"] = "content"

        workspace.wipe()

        assert workspace.files == {}
