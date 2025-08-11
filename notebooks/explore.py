import marimo

__generated_with = "0.14.16"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import matplotlib.pyplot as plt
    import seaborn as sns
    from config import PROCESSED_DIR, VISUALS_DIR
    return PROCESSED_DIR, VISUALS_DIR, mo, pl, plt, sns


@app.cell
def _(PROCESSED_DIR, pl):
    # Load data
    df = pl.read_parquet(PROCESSED_DIR / "cleaned_startup_berlin_28072025.parquet")
    return (df,)


@app.cell
def _(df):
    # Explore company locations
    df.select("city").unique()
    return


@app.cell
def _(df):
    df.select("id").n_unique()
    return


@app.cell
def _(df):
    df.shape
    return


@app.cell
def _(df):
    df.head(30)
    return


@app.cell
def _(mo):
    mo.md(r"""### Companies across years and cities""")
    return


@app.cell
def _(df):
    df_name = df.select(["name", "city", "launch_year", "type"]).unique(subset=["name"], keep="first")
    df_name
    return (df_name,)


@app.cell
def _(df_name, pl):
    city_counts = (df_name
        .group_by("city")
        .agg(pl.count("name").alias("company_count"))
                   .sort(by="company_count", descending=True)
                  )
    return (city_counts,)


@app.cell
def _(city_counts):
    city_counts
    return


@app.cell
def _(df_name, pl):
    df_berlin = df_name.filter(pl.col("city").str.contains("Berlin"))
    return (df_berlin,)


@app.cell
def _(df_berlin):
    df_berlin.select("city").unique()
    return


@app.cell
def _(mo):
    mo.md(r"""## Companies launched per year""")
    return


@app.cell
def _(df_berlin, pl):
    launch_counts = (df_berlin
                    .filter(pl.col("launch_year").is_not_null())
                     .group_by("launch_year")
                     .agg(pl.count("name").alias("company_count"))
                     .sort("launch_year")
                    )

    return (launch_counts,)


@app.cell
def _(launch_counts):
    launch_counts_pd = launch_counts.to_pandas()
    return (launch_counts_pd,)


@app.cell
def _(VISUALS_DIR, launch_counts_pd, plt, sns):
    sns.set_theme(style="whitegrid")
    sns.lineplot(data=launch_counts_pd, x="launch_year", y="company_count", marker="o")
    plt.title("Companies launched per year in Berlin")
    plt.xlabel("launch year")
    plt.ylabel("number of companies")
    plt.figtext(
        0.70, -0.05,  # x=70% width, y=just below x-axis
        "*Source: Startup Map Berlin, processed by my scraper\nData from 28.07.2025",
        ha="center", va="top", fontsize=9, color="gray"
    )
    plt.savefig(VISUALS_DIR / "companies_launched_per_year_in_berlin.png")
    plt.show()
    return


@app.cell
def _(launch_counts_pd):
    launch_counts_2010_pd = launch_counts_pd[launch_counts_pd.launch_year > 2010]
    return (launch_counts_2010_pd,)


@app.cell
def _(VISUALS_DIR, launch_counts_2010_pd, plt, sns):
    sns.set_theme(style="whitegrid")
    sns.lineplot(data= launch_counts_2010_pd, x = "launch_year", y = "company_count", marker = "o")
    plt.title("Companies launched per year in Berlin since 2010")
    plt.xlabel("launch year")
    plt.ylabel("number of companies")
    plt.figtext(
        0.70, -0.05,  # x=70% width, y=just below x-axis
        "*Source: Startup Map Berlin, processed by my scraper\nData from 28.07.2025",
        ha="center", va="top", fontsize=9, color="gray"
    )
    plt.savefig(VISUALS_DIR / "companies_launched_per_year_in_berlin_since_2010.png")
    plt.show()
    return


@app.cell
def _(mo):
    mo.md(r"""### Median valuation per year""")
    return


@app.cell
def _(df):
    df.head()
    return


@app.cell
def _(df, pl):
    df_valuation = (df
        .select(["name", "city", "launch_year", "growth_stage", "valuation", "valuation_market_cap"])
        .unique(subset=["name"], keep="first")
        .filter(pl.col("city").str.contains("Berlin"))
        .filter(pl.col("valuation").is_not_null())
                   )
    df_valuation
    return (df_valuation,)


@app.cell
def _(df_valuation):
    df_valuation.select("valuation_market_cap").unique()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
