## Simple Declarative Data Pipelines

Combine the simplicity of Trilogy with the tools of modern orchestration, such as DBT and Dagster.

> [!TIP]
> Pitch: don't worry about optimizing your ETL staging tables ever again - write your final tables and let TrilogyT handle the rest. 

Compile your models to ETL scripts to run on demand. Rebuild, run, and test easily.

Translates 'Persist' statements in Trilogy to scheduled ETL jobs. 

Currently supported backends:
- trilogy (optimize a Trilogy model, execute in memory)
- dbt (optimize a Trilogy model, translate it to a DBT project, optionally execute with dbt CLI)
- dagster (optimize a trilogy model, translate it to a dagster project, optionally  execute with dagster CLI)

> [!WARNING]
> This is an experimental library. The API is subject to change.

## Flags

--optimize=X - Any CTE used at least X times in calculating final model outputs will be materialized for reuse.

## Install

`pip install pytrilogyt`

## How to Run

preqlt <backend> <preql_path> <output_path> <backend> --run

## Trilogy

Optimize and execute a trilogy script (or set of scripts)

```bash
trilogyt trilogy trilogy/scripts trilogy/build bigquery --run
```


## DBT

For dbt, the output_path should be the root of the dbt project, where the dbt_project.yml file exists. 

An example command:

```bash
trilogyt dbt dbt/trilogy/ dbt bigquery --run
```

Each source .preql file will be built into a separate DBT sub folder with one model per persist statement. If optimization is enabled (default), there will 
be optional "optimization" tables that can be imported and depended on.

```bash
17:12:37  Running with dbt=1.7.4
17:12:38  Registered adapter: bigquery=1.7.2
17:12:38  Found 4 models, 4 tests, 0 sources, 0 exposures, 0 metrics, 447 macros, 0 groups, 0 semantic models
17:12:38
17:12:40  Concurrency: 4 threads (target='dev')
17:12:40
17:12:41  1 of 4 START sql view model dbt_test.customers ................................. [RUN]
17:12:41  2 of 4 START sql table model dbt_test.customers_preql_preqlt_gen_model ......... [RUN]
17:12:41  3 of 4 START sql table model dbt_test.my_first_dbt_model ....................... [RUN]
17:12:42  1 of 4 OK created sql view model dbt_test.customers ............................ [CREATE VIEW (0 processed) in 1.09s]
17:12:43  3 of 4 OK created sql table model dbt_test.my_first_dbt_model .................. [CREATE TABLE (2.0 rows, 0 processed) in 2.78s]
17:12:43  4 of 4 START sql view model dbt_test.my_second_dbt_model ....................... [RUN]
17:12:44  2 of 4 OK created sql table model dbt_test.customers_preql_preqlt_gen_model .... [CREATE TABLE (100.0 rows, 4.3 KiB processed) in 3.55s]
17:12:44  4 of 4 OK created sql view model dbt_test.my_second_dbt_model .................. [CREATE VIEW (0 processed) in 1.10s]
17:12:44
17:12:44  Finished running 2 view models, 2 table models in 0 hours 0 minutes and 6.37 seconds (6.37s).
17:12:45  
17:12:45  Completed successfully
17:12:45
17:12:45  Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4
customers: success
my_first_dbt_model: success
customers_preql_preqlt_gen_model: success
my_second_dbt_model: success
```

> [!TIP]
> Remember - you don't need to run the model with Trilogy. You can embed trilogy into a larger DBT project and use normal DBT tooling to manage the output. 

## Dagster

For Dagster, each source .preql file will be built as a model, with one entrypoint script that imports all resources.  If optimization is enabled (default), there will 
be optional "optimization" tables that can be imported and depended on.

An example command:

```bash
trilogyt dagster dbt/models/core/ ./dbt bigquery --run
```

> [!TIP]
> Remember - you don't need to run the model with Trilogy. You can embed a portion of Trilogy generated models into a larger Dagster project. 


### From IO

```console
Write-Output native """constant x <-5; persist into static as static select x;""" | trilogyt <output_path> bigquery
```