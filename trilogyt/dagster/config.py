from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from trilogyt.dagster.constants import SUFFIX


class DagsterConfig(BaseModel):
    root: Path
    build_tests: bool = True
    test_path: str = "tests"
    dagster_asset_path: str = "assets"
    namespace: Optional[str] = None

    def get_asset_path(self, key: str) -> Path:
        if self.namespace:
            output_path = (
                self.root / self.dagster_asset_path / self.namespace / f"{key}_{SUFFIX}"
            )
        else:
            output_path = self.root / self.dagster_asset_path / f"{key}_{SUFFIX}"
        return output_path

    @property
    def config_path(self) -> Path:
        if self.namespace:
            return self.root / self.dagster_asset_path / self.namespace / "schema.yml"
        return self.root / self.dagster_asset_path / "schema.yml"
