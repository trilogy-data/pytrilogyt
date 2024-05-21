from pydantic import BaseModel
from typing import Optional
from pathlib import Path


class DBTConfig(BaseModel):
    root: Path
    build_tests: bool = True
    test_path: str = "tests"
    dbt_model_path: str = "models"
    namespace: Optional[str] = None

    def get_model_path(self, key: str) -> Path:
        if self.namespace:
            output_path = (
                self.root
                / self.dbt_model_path
                / self.namespace
                / f"{key}_gen_model.sql"
            )
        else:
            output_path = self.root / self.dbt_model_path / f"{key}_gen_model.sql"
        return output_path

    @property
    def config_path(self) -> Path:
        if self.namespace:
            return self.root / self.dbt_model_path / self.namespace / "schema.yml"
        return self.root / self.dbt_model_path / "schema.yml"
