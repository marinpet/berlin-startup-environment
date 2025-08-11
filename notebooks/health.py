import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import seaborn as sns
    import matplotlib.pyplot as plt
    from config import PROCESSED_DIR
    return PROCESSED_DIR, mo, pl


@app.cell
def _(PROCESSED_DIR, pl):
    df = pl.read_parquet(PROCESSED_DIR / "processed_startup_berlin_28072025.parquet")
    df.head()
    return (df,)


@app.cell
def _(mo):
    mo.md(r"""### Select (sub)industries""")
    return


@app.cell
def _(df):
    df.select("sub_industry_name").unique().to_series().to_list()
    return


@app.cell
def _(df):
    df.select("industry").unique().to_series().to_list()
    return


@app.cell
def _():
    selected_subindustries = ["biotechnology", "health platform", "medical devices", None, "pharmaceutical", "fitness"]
    selected_industries = ["health"]
    return selected_industries, selected_subindustries


@app.cell
def _(df, pl, selected_industries, selected_subindustries):
    df_subset = (df
                .filter(pl.col("industry").is_in(selected_industries) | pl.col("sub_industry_name").is_in(selected_subindustries))
                )
    return (df_subset,)


@app.cell
def _(df_subset):
    df_subset.head(40)
    return


@app.cell
def _(df_subset):
    df_subset.select("growth_stage").unique()
    return


@app.cell
def _(mo):
    mo.md(r"""## Seed companies""")
    return


@app.cell
def _(df_subset, pl):
    seed = (df_subset
        .filter(pl.col("growth_stage") == "seed")
        .select(["name", "launch_year", "type", "is_ai_data", "growth_stage", "city", "industry", "sub_industry_name", "valuation_valuation"])
        .unique()
           )
    seed
    return (seed,)


@app.cell
def _(pl, seed):
    seed_ai_data = (seed.filter(pl.col("is_ai_data") == True))
    seed_ai_data
    return (seed_ai_data,)


@app.cell
def _(PROCESSED_DIR, seed_ai_data):
    seed_ai_data.write_csv(PROCESSED_DIR / "health_seed_ai_data.csv")
    return


@app.cell
def _(mo):
    mo.md(r"""## Early growth""")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
