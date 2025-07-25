import shutil
from pathlib import Path
from random import randint

from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    FileSystemImportResolver,
)


class BaseWorkspace:

    def __init__(self) -> None:
        # TODO: do something robust
        self.id = randint(0, 1000000)

    def get_files(self) -> dict[Path, str]:
        raise NotImplementedError("This method should be implemented by subclasses.")

    def get_file(self, path: Path) -> str:
        """Get a file from the workspace."""
        raise NotImplementedError("This method should be implemented by subclasses.")

    def file_exists(self, path: Path) -> bool:
        """Check if a file exists in the workspace."""
        raise NotImplementedError("This method should be implemented by subclasses.")

    def get_environment(self) -> Environment:
        raise NotImplementedError("This method should be implemented by subclasses.")

    def write_file(
        self,
        path: Path,
        content: str,
    ):
        """Write a file to the workspace."""
        raise NotImplementedError("This method should be implemented by subclasses.")

    def wipe(self):
        """Wipe the workspace."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class FileWorkspace(BaseWorkspace):
    def __init__(self, working_path: Path, paths: list[Path]):
        self.paths = paths
        self.working_path = working_path
        super().__init__()

    def file_exists(self, path: Path) -> bool:
        path = self.working_path / path
        return path.exists()

    def get_file(self, path: Path) -> str:
        with open(self.working_path / path, "r") as f:
            return f.read()

    def get_files(self):
        output: dict[Path, str] = {}
        for path in self.paths:
            output[path] = path.read_text()
        return output

    def get_environment(self) -> Environment:
        return Environment(
            working_path=self.working_path,
            import_resolver=FileSystemImportResolver(),
        )

    def write_file(
        self,
        path: Path,
        content: str,
    ):
        """Write a file to the workspace."""
        local_path = self.working_path / path
        local_path.write_text(content)
        self.paths.append(path)

    def wipe(self):
        if not self.working_path.exists():
            return
        for item in self.working_path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)  # Recursively removes directory and all contents


class MemoryWorkspace(BaseWorkspace):
    def __init__(self):
        self.files: dict[str, str] = {}
        super().__init__()

    def __repr__(self):
        return "MemoryWorkspace()"

    def file_exists(self, path: Path) -> bool:
        return str(path) in self.files

    def get_files(self) -> dict[Path, str]:
        return {Path(path): content for path, content in self.files.items()}

    def get_file(self, path: Path) -> str:
        return self.files[str(path)]

    def get_environment(self) -> Environment:
        return Environment(
            working_path=Path.cwd(),
            import_resolver=DictImportResolver(content=self.files),
        )

    def write_file(
        self,
        path: Path,
        content: str,
    ):
        """Write a file to the workspace."""
        self.files[str(path)] = content
        return path

    def wipe(self):
        self.files.clear()
