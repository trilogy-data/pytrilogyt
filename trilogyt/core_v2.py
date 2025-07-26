from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
from trilogy import Environment
from trilogy.parsing.render import Renderer
from trilogy.utility import unique
from trilogy.authoring import (
    PersistStatement,
    ConceptDeclarationStatement,
    ImportStatement,
    RowsetDerivationStatement,
)
from trilogy.core.models.author import HasUUID
from trilogyt.core import ENVIRONMENT_CONCEPTS

from trilogyt.constants import logger
from trilogyt.graph import process_raw
from trilogy.core.models.environment import Import
from trilogyt.fingerprint import ContentToFingerprintCache
from trilogyt.models import OptimizationInput, OptimizationResult
from trilogyt.io import BaseWorkspace, FileWorkspace
from trilogy.render import get_dialect_generator
from pathlib import Path
from typing import Any


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


class Optimizer:

    def __init__(self, materialization_threshold: int = 2, suffix: str = "_optimized"):
        self.materialization_threshold = materialization_threshold
        self.fingerprint_cache = ContentToFingerprintCache()
        self.suffix = suffix

    def paths_to_optimizations(
        self, dialect: Dialects, workspace: BaseWorkspace
    ) -> dict[str, OptimizationResult]:

        mapping: dict[Path, str] = {}
        for k, v in workspace.get_files().items():
            logger.info("Parsing file: %s", k)
            mapping[k] = v

        return self.mapping_to_optimizations(workspace, dialect, mapping)

    def write_imports(
        self,
        imports: dict[str, list[Import]],
        workspace: BaseWorkspace,
        target_workspace: BaseWorkspace,
    ):
        if not imports:
            return
        if isinstance(target_workspace, FileWorkspace):
            logger.info("Writing imports to path: %s", target_workspace.working_path)

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
                target_path = PathlibPath(PathlibPath(imp.input_path).name)
                if target_workspace.file_exists(target_path):
                    continue
                content = workspace.get_file(imp.input_path)

                env, _ = workspace.get_environment().parse(content)
                self.write_imports(env.imports, workspace, target_workspace)
                target_workspace.write_file(target_path, content)

    def optimizations_to_files(
        self,
        optimizations: dict[str, OptimizationResult],
        workspace: BaseWorkspace,
        output_workspace: BaseWorkspace | None = None,
    ):
        target = output_workspace or workspace
        for k, opt in optimizations.items():
            if not opt.build_content.strip():
                continue
            target.write_file(
                PathlibPath(opt.filename + "_build").with_suffix(".preql"),
                opt.build_content,
            )
            target.write_file(
                PathlibPath(opt.filename).with_suffix(".preql"), opt.import_content
            )
            if opt.imports:
                self.write_imports(opt.imports, workspace, target)

        return

    def rewrite_files_with_optimizations(
        self,
        workspace: BaseWorkspace,
        optimizations: dict[str, OptimizationResult],
        output_workspace: BaseWorkspace | None = None,
    ):
        target_workspace = output_workspace or workspace
        for file in workspace.get_files():
            content = workspace.get_file(file)
            fingerprint, _, _ = self._content_to_fingerprint(workspace, content)
            if fingerprint is None:
                continue
            optimization = optimizations.get(fingerprint)
            if not optimization:
                new = content
                env, _ = workspace.get_environment().parse(content)
                if env.imports:
                    self.write_imports(env.imports, workspace, target_workspace)
            else:

                new = self._apply_optimization(
                    content, optimization, workspace, target_workspace
                )
            file = Path(f"{file.stem}{self.suffix}{file.suffix}")
            logger.info("Writing optimized file: %s", file)
            target_workspace.write_file(
                file,
                new,
            )

    def _apply_optimization(
        self,
        content: str,
        optimization: OptimizationResult,
        workspace: BaseWorkspace,
        target_workspace: BaseWorkspace | None = None,
    ) -> str:
        target_workspace = target_workspace or workspace
        fingerprint, statements, env = self._content_to_fingerprint(workspace, content)
        renderer = Renderer(environment=env)
        statements = [
            ImportStatement(
                alias="",
                input_path=optimization.filename,
                path=optimization.filename,  # type:ignore
            )
        ] + [
            x
            for x in statements
            if not isinstance(x, (ImportStatement, RowsetDerivationStatement))
        ]
        strings = []
        for x in statements:

            strings.append(renderer.to_string(x))
        return "\n\n".join(strings)

    def _content_to_fingerprint(
        self, workspace: BaseWorkspace, content: str
    ) -> tuple[str | None, list[Any], Environment]:

        return self.fingerprint_cache.content_to_fingerprint(
            workspace=workspace, content=content
        )

    def mapping_to_optimizations(
        self, workspace: BaseWorkspace, dialect: Dialects, contents: dict[Path, str]
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
                    # don't mutate existing statement list
                    statements=[*statements],
                    imports=new_env.imports,
                )

        return self.optimization_inputs_to_outputs(env_to_statements, dialect)

    def optimization_inputs_to_outputs(
        self,
        mapping: dict[str, OptimizationInput],
        dialect: Dialects,
    ) -> dict[str, OptimizationResult]:

        outputs: dict[str, OptimizationResult] = {}

        for k, v in mapping.items():
            _, new_persists = process_raw(
                inject=False,
                inputs=v.statements,
                env=v.environment,
                generator=get_dialect_generator(
                    dialect,
                ),
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
            build_strings: list[str] = []
            import_strings: list[str] = []
            renderer = Renderer(environment=v.environment)
            for concept in ENVIRONMENT_CONCEPTS:
                build_strings.append(
                    renderer.to_string(ConceptDeclarationStatement(concept=concept))
                )
                import_strings.append(
                    renderer.to_string(ConceptDeclarationStatement(concept=concept))
                )
            for statement in final:
                build_strings.append(renderer.to_string(statement))
                import_strings.append(renderer.to_string(statement))
            for x in new_persists:
                build_strings.append(renderer.to_string(x))
                import_strings.append(renderer.to_string(x.datasource))
                # f.write(renderer.to_string(x.datasource) + "\n\n")

            outputs[k] = OptimizationResult(
                fingerprint=k,
                build_content="\n\n".join(build_strings),
                import_content="\n\n".join(import_strings),
                imports=v.imports,
            )

        return outputs
