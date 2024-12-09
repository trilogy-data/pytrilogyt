from trilogyt.core import enrich_environment
from trilogyt.dbt.generate import generate_model as generate_dbt_model
from trilogyt.dbt.config import DBTConfig

try:
    from trilogyt.dagster import generate_model as generate_dagster_model, DagsterConfig
except ImportError:
    generate_dagster_model = None
    DagsterConfig = None
    pass

__version__ = "0.0.11"

__all__ = ["enrich_environment", "generate_dbt_model", "DBTConfig", "generate_dagster_model", "DagsterConfig"]
