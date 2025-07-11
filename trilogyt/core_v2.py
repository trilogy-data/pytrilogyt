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
from trilogyt.models import OptimizationInput, OptimizationResult
from trilogyt.io import BaseWorkspace


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


class Optimizer:

    def __init__(self, materialization_threshold: int = 2):
        self.materialization_threshold = materialization_threshold
        self.fingerprint_cache = ContentToFingerprintCache()

    def paths_to_optimizations(
        self, working_path: PathlibPath, dialect: Dialects, workspace: BaseWorkspace
    ) -> dict[str, OptimizationResult]:

        mapping: dict[str, str] = {}
        for k, v in workspace.get_files():
            logger.info("Parsing file: %s", k)
            mapping[k] = v

        return self.mapping_to_optimizations(workspace, dialect, mapping)

    def wipe_directory(self, path: PathlibPath):
        if not path.exists():
            return
        logger.info("Wiping directory: %s", path)
        for item in path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                os.rmdir(item)

    def write_imports(self, imports: dict[str, list[Import]], target: PathlibPath):
        if not imports:
            return
        logger.info("Writing imports to path: %s", target)

        for k, imp_list in imports.items():
            if not imp_list:
                continue
            for imp in imp_list:
                if not imp.input_path:
                    continue
                logger.info(
                    "Processing import: %s for optimization: %s", imp.input_path, k
                )
                # check if target has been written
                # if it has, we can skip
                if PathlibPath(target / PathlibPath(imp.input_path).name).exists():
                    continue
                with open(imp.input_path, "r") as input_file:
                    content = input_file.read()
                    env, _ = Environment(working_path=target.parent).parse(content)
                    self.write_imports(env.imports, target)
                    with open(target / PathlibPath(imp.input_path).name, "w") as f:
                        f.write(content)

    def optimizations_to_files(
        self,
        optimizations: dict[str, OptimizationResult],
        path: PathlibPath,
        output_path: PathlibPath | None = None,
    ):
        target = output_path or path
        for k, opt in optimizations.items():
            if not opt.content.strip():
                continue

            file_path = target / PathlibPath(opt.filename).with_suffix(".preql")
            logger.info("Writing optimization to file: %s", file_path)
            with open(file_path, "w") as f:
                f.write(opt.content)
            if opt.imports:
                self.write_imports(opt.imports, target)

        return

    def rewrite_files_with_optimizations(
        self,
        workspace: BaseWorkspace,
        files: list[PathlibPath],
        optimizations: dict[str, OptimizationResult],
        suffix: str = "_optimized",
        output_workspace: BaseWorkspace | None = None,
    ):
        target_workspace = output_workspace or workspace
        for file in files:
            logger.info(
                "Parsing file: %s",
                file,
            )
            with open(file) as f:
                content = f.read()
            fingerprint, _, _ = self._content_to_fingerprint(target_workspace, content)
            if fingerprint is None:
                continue
            optimization = optimizations.get(fingerprint)
            if not optimization:
                new = content
                env, _ = target_workspace.get_environment().parse(content)
                if env.imports:
                    self.write_imports(env.imports, target_workspace)
            else:
                new = self._apply_optimization(
                    content, optimization, workspace, target_workspace
                )

            target_workspace.write_file(
                file,
                new,
                suffix=suffix,
            )


    def _apply_optimization(
        self,
        content: str,
        optimization: OptimizationResult,
        working_path: PathlibPath,
        target_path: PathlibPath | None = None,
    ) -> str:
        target_path = target_path or working_path
        fingerprint, statements, env = self._content_to_fingerprint(
            working_path, content
        )
        renderer = Renderer(environment=env)
        statements = [
            ImportStatement(
                alias="",
                input_path=str(target_path / optimization.filename),
                path=optimization.filename,
            )
        ] + [x for x in statements if not isinstance(x, ImportStatement)]
        strings = []
        for x in statements:

            strings.append(renderer.to_string(x))
        return "\n\n".join(strings)

    def _content_to_fingerprint(
        self, workspace:BaseWorkspace, content: str
    ) -> tuple[str | None, list[any], Environment]:

        return self.fingerprint_cache.content_to_fingerprint(
            workspace=workspace, content=content
        )

    def mapping_to_optimizations(
        self, workspace:BaseWorkspace, dialect: Dialects, contents: dict[str, str]
    ) -> dict[str, OptimizationResult]:
        env_to_statements: dict[str, OptimizationInput] = {}
        key_to_fingerprint = {}
        for key, content in contents.items():
            # to look at first - fingerprint does not include local select overrides
            fingerprint, statements, new_env = self._content_to_fingerprint(
                workspace, content
            )
            # if there is no fingerprint, skip
            if not fingerprint:
                continue
            key_to_fingerprint[key] = fingerprint
            if fingerprint in env_to_statements:
                opt: OptimizationInput = env_to_statements[fingerprint]
                opt.statements += statements
            else:
                env_to_statements[fingerprint] = OptimizationInput(
                    fingerprint=fingerprint,
                    environment=new_env,
                    statements=statements,
                    imports=new_env.imports,
                )

        return self.optimization_inputs_to_outputs(
            workspace, env_to_statements, dialect
        )

    def optimization_inputs_to_outputs(
        self,
        working_path: PathlibPath,
        mapping: dict[str, OptimizationInput],
        dialect: Dialects,
    ) -> dict[str, OptimizationResult]:
        optimize_env = Environment(working_path=working_path)
        exec = Executor(
            dialect=dialect, engine=dialect.default_engine(), environment=optimize_env
        )

        outputs: dict[str, OptimizationResult] = {}

        for k, v in mapping.items():
            _, new_persists = process_raw(
                inject=False,
                inputs=v.statements,
                env=v.environment,
                generator=exec.generator,
                threshold=self.materialization_threshold,
            )
            # don't create a file if there is nothing to persist
            if not new_persists:
                continue

            concept_modifying_statements = unique(
                [x for x in v.statements if isinstance(x, HasUUID)], "uuid"
            )
            pre_final: list[HasUUID] = []
            # we should transform a persist into a select for optimization purposes
            for x in concept_modifying_statements:
                # if isinstance(x, (RowsetDerivationStatement, SelectStatement, ImportStatement, MergeStatementV2, ConceptDeclarationStatement)):
                #     final.append(x)
                # add this to get the definition ins
                if isinstance(x, PersistStatement):
                    pre_final.append(x.select)
                else:
                    pre_final.append(x)
            seen = set()
            final = []
            for x in pre_final:
                if x.uuid not in seen:
                    seen.add(x.uuid)
                    final.append(x)
            strings: list[str] = []
            renderer = Renderer(environment=v.environment)
            for concept in ENVIRONMENT_CONCEPTS:
                strings.append(
                    renderer.to_string(ConceptDeclarationStatement(concept=concept))
                )
            for statement in final:
                strings.append(renderer.to_string(statement))
            for x in new_persists:
                strings.append(renderer.to_string(x))
                # f.write(renderer.to_string(x.datasource) + "\n\n")

            outputs[k] = OptimizationResult(
                fingerprint=k, content="\n\n".join(strings), imports=v.imports
            )

        return outputs
