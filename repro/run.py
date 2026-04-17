from pathlib import Path

from trilogy import Dialects, Environment

HERE = Path(__file__).parent


def make_engine():
    env = Environment(working_path=HERE)
    engine = Dialects.DUCK_DB.default_executor(environment=env)

    engine.execute_raw_sql(
        "CREATE TABLE items_tbl (id INT, category VARCHAR, price INT)"
    )
    engine.execute_raw_sql(
        "INSERT INTO items_tbl VALUES "
        "(1,'A',10),(2,'A',20),(3,'A',100),(4,'B',30),(5,'B',40)"
    )
    engine.execute_raw_sql(
        "CREATE TABLE sales_tbl (id INT, year INT, item_id INT, item_price INT, item_category VARCHAR)"
    )
    engine.execute_raw_sql(
        "INSERT INTO sales_tbl VALUES "
        "(1,2023,1,10,'A'),(2,2023,2,20,'A'),(3,2023,4,30,'B'),(4,2023,5,40,'B'),"
        "(5,2022,3,100,'A')"
    )

    engine.execute_raw_sql(
        """
        CREATE TABLE staged_sales_tbl AS
        SELECT s.id AS sale_id, s.year AS sale_year,
               i.id AS item_id, i.category AS item_category,
               i.price AS sales_item_price, i.category AS sales_item_category
        FROM sales_tbl s JOIN items_tbl i ON s.item_id = i.id
        WHERE s.year = 2023
        """
    )
    return engine


def run_query(engine, preql_path: Path, label: str) -> None:
    engine.environment = Environment(working_path=HERE)
    with open(preql_path) as f:
        text = f.read()
    queries = engine.parse_text(text)
    sql = engine.generate_sql(queries[-1])[-1]
    result = engine.execute_raw_sql(sql).fetchall()
    print(f"\n=== {label} ===")
    print(f"file: {preql_path.name}")
    print(f"result: {result}")
    print(f"--- generated sql ---\n{sql}")


def main():
    engine = make_engine()
    run_query(engine, HERE / "query_base.preql", "BASE (expected)")
    run_query(engine, HERE / "query_staged.preql", "STAGED (bug)")


if __name__ == "__main__":
    main()
