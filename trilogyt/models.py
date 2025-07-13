from dataclasses import dataclass
from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from trilogy import Environment, Executor
from trilogy.parsing.render import Renderer
from trilogy.utility import unique
from trilogy.authoring import (
    PersistStatement,
    ConceptDeclarationStatement,
    ImportStatement,
)
from trilogy.core.models.author import HasUUID
from dataclasses import dataclass
from trilogyt.core import ENVIRONMENT_CONCEPTS

from trilogyt.constants import logger, OPT_PREFIX
from trilogyt.graph import process_raw
from trilogy.core.models.environment import Import
from trilogyt.fingerprint import ContentToFingerprintCache



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
