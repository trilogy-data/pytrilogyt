from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
from trilogy import Environment
from trilogy.parsing.parse_engine import parse_text
from trilogy.authoring import (
    PersistStatement,
    SelectStatement,
    ImportStatement,
)
from trilogy.core.statements.author import CopyStatement
from trilogy.core.models.author import HasUUID
from trilogyt.core import fingerprint_environment
from trilogyt.io import BaseWorkspace
import hashlib


class ContentToFingerprintCache:
    def __init__(self, max_cache_size: int = 1000):
        self._cache: dict[str, tuple[str, list[HasUUID], "Environment"]] = {}
        self.max_cache_size = max_cache_size

    def _get_content_hash(self, content: str, workspace:BaseWorkspace) -> str:
        """Generate a hash of the input content and working path."""
        # Combine content and working_path for the hash since working_path affects the result
        combined_input = f"{workspace.id}:{content}"
        return hashlib.sha256(combined_input.encode("utf-8")).hexdigest()

    def _evict_oldest_if_needed(self):
        """Simple LRU eviction - remove oldest entries if cache is full."""
        if len(self._cache) >= self.max_cache_size:
            # Remove the first item (oldest in insertion order for Python 3.7+)
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]

    def content_to_fingerprint(
        self, workspace:BaseWorkspace, content: str
    ) -> tuple[str, list[HasUUID], "Environment"]:
        # Generate hash of input
        content_hash = self._get_content_hash(content, workspace)

        # Check cache first
        if content_hash in self._cache:
            cached_result = self._cache[content_hash]
            return (cached_result[0], cached_result[1], cached_result[2])

        # If not in cache, compute the result
        local_env = workspace.get_environment()

        try:
            new_env, statements = parse_text(content, environment=local_env)
        except Exception as e:
            raise e

        if not any(
            isinstance(statement, (SelectStatement, PersistStatement, CopyStatement))
            for statement in statements
        ):
            result = (None, statements, new_env)
        else:
            build_env = new_env.materialize_for_select({})
            human_labels = []
            for statement in statements:
                if isinstance(statement, ImportStatement):
                    human_labels.append(
                        (statement.alias or statement.input_path).replace(".", "_")
                    )
            fingerprint = fingerprint_environment(build_env)
            name = "_".join(human_labels[:2])
            if len(human_labels) > 2:
                name += f"_{len(human_labels) - 2}_more"
            result = (name + "_" + fingerprint[0:8], statements, new_env)

        # Cache the result before returning
        self._evict_oldest_if_needed()
        self._cache[content_hash] = result

        return result

    def clear_cache(self):
        """Clear all cached results."""
        self._cache.clear()
