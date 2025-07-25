from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from trilogyt.dagster.constants import SUFFIX

DEFINITIONS_FILE = "definitions.py"


class DagsterConfig(BaseModel):
    root: Path
    build_tests: bool = True
    test_path: str = "tests"
    dagster_asset_path: str = "src/defs"
    namespace: Optional[str] = None
    opt_import_root: str = "src.defs.optimization"

    def get_asset_import_path(
        self,
        key: str,
    ) -> Path:
        if self.namespace:
            output_path = (
                Path(self.dagster_asset_path) / self.namespace / f"{key}_{SUFFIX}"
            )
        else:
            output_path = Path(self.dagster_asset_path) / f"{key}_{SUFFIX}"
        return output_path

    def get_asset_path(self, key: str) -> Path:
        if self.namespace:
            output_path = (
                self.root / self.dagster_asset_path / self.namespace / f"{key}_{SUFFIX}"
            )
        else:
            output_path = self.root / self.dagster_asset_path / f"{key}_{SUFFIX}"
        return output_path

    def get_entrypoint_path(self) -> Path:
        return self.root / "src" / DEFINITIONS_FILE

    @property
    def config_path(self) -> Path:
        if self.namespace:
            return self.root / self.dagster_asset_path / self.namespace / "dagster.yaml"
        return self.root / self.dagster_asset_path / "dagster.yaml"
