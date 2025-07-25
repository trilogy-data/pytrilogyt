from pathlib import Path

from networkx import DiGraph, topological_sort
from trilogy import Dialects, Environment

from trilogyt.constants import logger
from trilogyt.scripts.core import OptimizationResult


def generate_execution_order(edges) -> list[Path]:
    graph: DiGraph = DiGraph()
    for edge in edges:
        graph.add_edge(*edge)
    return list(topological_sort(graph))


def run_path(
    path: Path,
    dialect: Dialects,
    env_to_optimization: dict[Path, OptimizationResult] | None = None,
):
    # initialize
    files = path.glob("*.preql")
    edges: list[tuple[Path, Path]] = []
    executor = dialect.default_executor()
    for x in files:
        try:
            env = Environment(working_path=path)
            executor.environment = env
            executor.parse_file(x)
            for _, imp_list in env.imports.items():
                for imp in imp_list:
                    target = (path / imp.path).with_suffix(".preql")
                    build_path = target.with_stem(f"{imp.path.stem}_build")
                    if build_path.exists():
                        edges.append((build_path, target))
                    edges.append((target, x))

        except Exception as e:
            logger.error(f" Error executing {x} {e}")
            raise e

    if env_to_optimization:
        logger.info("Have optimization scripts, running builds for datasources first")
        opt_build_scripts = set()
        for _, v in env_to_optimization.items():
            opt_build_scripts.add(v.path)
        for script in opt_build_scripts:
            env = Environment(working_path=path)
            executor.environment = env
            logger.info(f"Executing optimization script: {script}")
            executor.execute_file(script, non_interactive=True)
    sorted_files: list[Path] = generate_execution_order(edges)
    logger.info(edges)
    for file in sorted_files:
        env = Environment(working_path=path)
        executor.environment = env
        if not file.suffix == "preql":
            file = file.with_suffix(".preql")
        logger.info(f"Executing file: {file}")
        executor.execute_file(path / file, non_interactive=True)
