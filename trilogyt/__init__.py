from trilogy.constants import CONFIG

from trilogyt.core import enrich_environment
from trilogyt.dagster.config import DagsterConfig
from trilogyt.dagster.generate import generate_model as generate_dagster_model
from trilogyt.dbt.config import DBTConfig
from trilogyt.dbt.generate import generate_model as generate_dbt_model

# Do not use parameters when compiling SQL
# TODO: make this local to environments?
# likely requires trilogy feature request
CONFIG.rendering.parameters = False

__version__ = "0.0.13"

__all__ = [
    "enrich_environment",
    "generate_dbt_model",
    "DBTConfig",
    "generate_dagster_model",
    "DagsterConfig",
]
