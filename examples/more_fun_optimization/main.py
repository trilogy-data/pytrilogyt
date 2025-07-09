import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from logging import StreamHandler

from trilogy import Dialects

from trilogyt.constants import logger
from trilogyt.core_v2 import Optimizer
from trilogyt.native.run import run_path_v2

logger.addHandler(StreamHandler())
logger.setLevel("DEBUG")

root = Path(__file__).parent
output_path = root / "output"
if not output_path.exists():
    output_path.mkdir(parents=True, exist_ok=True)

optimizer = Optimizer()


files = root.glob("*.preql")

sources = [
    x for x in files if not x.stem.startswith("_") and not x.stem.endswith("_optimized")
]

logger.info(f"Have {len(sources)} files")

optimizations = optimizer.paths_to_optimizations(
    working_path=root, files=sources, dialect=Dialects.DUCK_DB
)

for k, opt in optimizations.items():
    print(f"Fingerprint: {opt.fingerprint}")
    print(f"Content:\n{opt.content}\n")
    print(f"Imports: {opt.imports}\n")
    # Here you can save the content to a file or process it further
    # For example, you could write to a file:
    # with open(f"{opt.fingerprint}.preql", "w") as f:
    #     f.write(opt.content)

optimizer.wipe_directory(output_path)
optimizer.optimizations_to_files(optimizations, root, output_path)


optimizer.rewrite_files_with_optimizations(
    root, sources, optimizations, "_optimized", output_path
)

run_path_v2(
    output_path,
    Dialects.DUCK_DB,
)

# def get_optimizations(logger):
#     fake = root.parent / "native"
#     os.makedirs(fake, exist_ok=True)
#     assert fake.exists()
#     native_wrapper(
#         root.parent,
#         root.parent / "output",
#         Dialects.DUCK_DB,
#         run=True,
#         debug=False,
#     )

#     results = root.parent / "native"
#     output = list(results.glob("**/*.preql"))
#     assert len(output) == 10, [f for f in output]
#     for f in output:
#         # our generated file
#         if "dim_splits" not in str(f):
#             continue
#         if f.is_file():
#             with open(f) as file:
#                 content = file.read()
#                 # validate we are using our generated model
#                 assert "import optimize" in content, content

# load a file

# write the cached datasources

# write an updated file that uses the imports
