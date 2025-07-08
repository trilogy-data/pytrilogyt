from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from sys import path as sys_path
from trilogy import Environment, Executor
from trilogy.parsing.parse_engine import parse_text
from trilogy.parsing.render import Renderer
from trilogy.utility import unique
from trilogy.authoring import (
    PersistStatement,
    SelectStatement,
    ConceptDeclarationStatement,
    RowsetDerivationStatement,
    ImportStatement,
)
from trilogy.core.statements.author import CopyStatement
from trilogy.core.models.author import HasUUID
from dataclasses import dataclass
from trilogyt.core import ENVIRONMENT_CONCEPTS, fingerprint_environment

from trilogyt.constants import OPTIMIZATION_FILE, logger, OPT_PREFIX
from trilogyt.graph import process_raw
from trilogyt.exceptions import OptimizationError
from trilogy.core.models.environment import Import


@dataclass
class OptimizationInput:
    fingerprint: str
    environment: Environment
    statements: list
    imports: dict[str, list[Import]] | None = None


@dataclass
class OptimizationResult:
    fingerprint: str
    content: str
    imports: dict[str, list[Import]] | None = None

    @property
    def filename(self) -> str:
        return f"{OPT_PREFIX}{self.fingerprint}"





def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


class Optimizer:

    def __init__(self, materialization_threshold: int = 2):
        self.materialization_threshold = materialization_threshold

    def paths_to_optimizations(
        self, working_path: PathlibPath, dialect: Dialects, files: list[PathlibPath]
    ) -> dict[str, OptimizationResult]:

        mapping: dict[str, str] = {}
        for file in files:
            logger.info("Parsing file: %s", file)
            with open(file) as f:
                mapping[str(file)] = f.read()

        return self.mapping_to_optimizations(working_path, dialect, mapping)

    def wipe_directory(self, path: PathlibPath):
        if not path.exists():
            return
        logger.info("Wiping directory: %s", path)
        for item in path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                os.rmdir(item)
    def write_imports(self, imports:dict[str, list[Import]], target: PathlibPath):
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
                    "Processing import: %s for optimization: %s",
                    imp.input_path, k)
                with open(imp.input_path, "r") as input_file:
                    content = input_file.read()
                    env, _ = Environment(working_path=target.parent).parse(
                        content
                    )
                    self.write_imports(env.imports, target)
                    with open(
                        target / PathlibPath(imp.input_path).name, 'w') as f:
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
        working_path: PathlibPath,
        files: list[PathlibPath],
        optimizations: dict[str, OptimizationResult],
        suffix: str = "_optimized",
        output_path: PathlibPath | None = None,
    ):
        target_path = output_path or working_path
        for file in files:
            logger.info(
                "Parsing file: %s",
                file,
            )
            with open(file) as f:
                content = f.read()
            fingerprint, _, _ = self._content_to_fingerprint(working_path, content)
            if fingerprint is None:
                continue
            optimization = optimizations.get(fingerprint)
            if not optimization:
                new = content
            else:
                new = self._apply_optimization(
                    content, optimization, working_path, target_path
                )
            if output_path is not None:
                new_path = (
                    output_path / file.with_name(file.stem + suffix + file.suffix).name
                )
            else:
                new_path = file.with_name(file.stem + suffix + file.suffix)
            with open(new_path, "w") as f:
                logger.info(
                    "Rewriting file: %s with optimization %s", file, fingerprint
                )
                f.write(new)

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
        self, working_path, content: str
    ) -> tuple[str | None, list[any], Environment]:
        local_env = Environment(
            working_path=working_path,
        )
        try:
            new_env, statements = parse_text(content, environment=local_env)
        except Exception as e:
            raise e
        if not any(
            isinstance(statement, (SelectStatement, PersistStatement, CopyStatement))
            for statement in statements
        ):
            return None, statements, new_env
        build_env = new_env.materialize_for_select({})
        human_labels = []
        for statement in statements:
            if isinstance(statement, ImportStatement):
                human_labels.append((statement.alias or statement.input_path).replace('.', '_'))
        fingerprint = fingerprint_environment(build_env)
        name = '_'.join(human_labels[:2])
        if len(human_labels) > 2:
            name += f"_{len(human_labels) - 2}_more"
        return name+'_'+fingerprint[0:8], statements, new_env

    def mapping_to_optimizations(
        self, working_path: PathlibPath, dialect: Dialects, contents: dict[str, str]
    ) -> dict[str, OptimizationResult]:
        env_to_statements: dict[str, OptimizationInput] = {}
        key_to_fingerprint = {}
        for key, content in contents.items():
            # to look at first - fingerprint does not include local select overrides
            fingerprint, statements, new_env = self._content_to_fingerprint(
                working_path, content
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
            working_path, env_to_statements, dialect
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


def optimize_multiple(
    base: PathlibPath,
    paths: list[PathlibPath],
    dialect: Dialects,
) -> list[OptimizationResult]:

    optimize_env = Environment(working_path=base.stem)
    exec = Executor(
        dialect=dialect, engine=dialect.default_engine(), environment=optimize_env
    )

    env_to_statements: dict[str, OptimizationInput] = {}
    file_to_fingerprint = {}
    for path in paths:
        if path.name.startswith(OPTIMIZATION_FILE):
            continue
        with open(path) as f:
            local_env = Environment(
                working_path=path.parent,
            )
            try:
                new_env, statements = parse_text(f.read(), environment=local_env)
            except Exception as e:
                raise SyntaxError(f"Unable to parse {path} due to {e}")
            if not any(
                isinstance(
                    statement, (SelectStatement, PersistStatement, CopyStatement)
                )
                for statement in statements
            ):
                continue
            build_env = new_env.materialize_for_select({})
            fingerprint = fingerprint_environment(build_env)
            file_to_fingerprint[path] = fingerprint
            if fingerprint in env_to_statements:
                opt: OptimizationInput = env_to_statements[fingerprint]
                opt.statements += statements
            else:
                env_to_statements[fingerprint] = OptimizationInput(
                    fingerprint=fingerprint, environment=new_env, statements=statements
                )

    # determine the new persists we need to create
    outputs: list[OptimizationResult] = []
    for k, v in env_to_statements.items():
        _, new_persists = process_raw(
            inject=False,
            inputs=v.statements,
            env=v.environment,
            generator=exec.generator,
            threshold=2,
        )

        concept_modifying_statements = unique(
            [x for x in v.statements if isinstance(x, HasUUID)], "uuid"
        )
        final: list[HasUUID] = []
        # we should transform a persist into a select for optimization purposes
        for x in concept_modifying_statements:
            if isinstance(x, PersistStatement):
                final.append(x.select)
            else:
                final.append(x)
        strings: list[str] = []
        for concept in ENVIRONMENT_CONCEPTS:
            strings.append(
                renderer.to_string(ConceptDeclarationStatement(concept=concept))
                + "\n\n"
            )
        for cte in final:
            strings.append(renderer.to_string(cte) + "\n\n")
        for x in new_persists:
            strings.append(renderer.to_string(x) + "\n\n")
            # f.write(renderer.to_string(x.datasource) + "\n\n")

        outputs.append(OptimizationResult(fingerprint=k, content="\n\n".join(strings)))
    return outputs
