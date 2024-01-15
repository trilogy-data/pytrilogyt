from pydantic import BaseModel
from typing import Optional
from pathlib import Path

class DBTConfig(BaseModel):
    root: Path
    build_tests: bool = True
    test_path: str = "tests"
    model_path: str = "models"
    namespace: Optional[str] = None
