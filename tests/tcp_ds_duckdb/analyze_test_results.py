from pathlib import Path
import tomllib
import os

if __name__ == "__main__":

    import pandas as pd
    import matplotlib.pyplot as plt

    results = []
    root = Path(__file__).parent
    for filename in os.listdir(root):
        if filename.endswith(".log"):
            with open(root / filename, "r") as f:
                try:
                    loaded = tomllib.loads(f.read())
                except tomllib.TOMLDecodeError:
                    continue
                results.append(loaded)
                print(f"----{filename}----")
                print(loaded["optimized_generated_sql"])
                print("-------")

    df = pd.DataFrame.from_records(results)

    print(df)

    # Plot the results
    fig, ax = plt.subplots()
    ax.set_title("Query execution time")
    ax.set_xlabel("Query")
    ax.set_ylabel("Execution time (s)")

    df["query_id"] = df["query_id"].astype("category")
    df["query_id"] = df["query_id"].cat.set_categories(df["query_id"].unique())

    df = df.sort_values("query_id")

    ax.boxplot(
        [df["optimized_exec_time"], df["base_exec_time"], df["sql_comp_time"]],
        tick_labels=["Base", "Optimized", "Non_trilogy"],
    )

    plt.show()
