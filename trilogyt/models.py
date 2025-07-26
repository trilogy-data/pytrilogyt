from dataclasses import dataclass
from pathlib import Path as PathlibPath  # noqa

from trilogy import Environment
from trilogy.core.models.environment import Import
from trilogy.dialect.enums import Dialects  # noqa

from trilogyt.constants import OPT_PREFIX


@dataclass
class OptimizationInput:
    fingerprint: str
    environment: Environment
    statements: list
    imports: dict[str, list[Import]] | None = None


@dataclass
class OptimizationResult:
    fingerprint: str
    build_content: str
    import_content: str
    imports: dict[str, list[Import]] | None = None

    @property
    def filename(self) -> str:
        return f"{OPT_PREFIX}{self.fingerprint}"
