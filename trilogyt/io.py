from pathlib import Path
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    FileSystemImportResolver,
    Import,
)
from random import randint

class BaseWorkspace():

    def __init__(self) -> None:
        # TODO: do something robust
        self.id = randint(0, 1000000)

    def get_files(self)->dict[str, str]:
        raise NotImplementedError("This method should be implemented by subclasses.")
    
    def get_environment(self) -> Environment:
        raise NotImplementedError("This method should be implemented by subclasses.")
    
    def write_file(
            self,
            path: Path,
            content:str,
            suffix:str,
    ):
        """Write a file to the workspace."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class FileWorkspace(BaseWorkspace):
    def __init__(self, working_path, paths: list[Path]):
        self.paths = paths
        self.working_path = working_path
        super().__init__()


    def get_files(self):
        output = {}
        for path in self.paths:
            output[str(path)] = path.read_text()
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
        
        path.write_text(content)
        self.paths.append(path)
    

class MemoryWorkspace(BaseWorkspace):
    def __init__(self):
        self.files = {}

    def __repr__(self):
        return "MemoryWorkspace()"

    def get_files(self) -> dict[str, str]:
        return self.files
    

    
    def get_environment(self) -> Environment:
        return Environment(
            working_path=Path.cwd(),
            import_resolver=DictImportResolver(content=self.files),
        )
    
    def write_file(
            self,
            path: Path,
            content: str,
            suffix: str = "optimized",
    ):
        """Write a file to the workspace."""
        file_path = f"{path.stem}{suffix}{path.suffix}"
        self.files[file_path] = content
        return file_path