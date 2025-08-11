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
def _(mo):
    mo.md(r"""## Companies launched per year""")
    return


@app.cell
def _(df, pl):
    launch_counts = (df
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
    plt.title("Companies launched per year")
    plt.xlabel("launch year")
    plt.ylabel("number of companies")
    plt.figtext(
        0.70, -0.05,  # x=70% width, y=just below x-axis
        "*Source: Startup Map Berlin, processed by my scraper\nData from 28.07.2025",
        ha="center", va="top", fontsize=9, color="gray"
    )
    plt.savefig(VISUALS_DIR / "companies_launched_per_year.png")
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
    plt.title("Companies launched per year since 2010")
    plt.xlabel("launch year")
    plt.ylabel("number of companies")
    plt.figtext(
        0.70, -0.05,  # x=70% width, y=just below x-axis
        "*Source: Startup Map Berlin, processed by my scraper\nData from 28.07.2025",
        ha="center", va="top", fontsize=9, color="gray"
    )
    plt.savefig(VISUALS_DIR / "companies_launched_per_year_since_2010.png")
    plt.show()
    return


@app.cell
def _(mo):
    mo.md(r"""### Industry growth over time""")
    return


@app.cell
def _(df):
    df.head()
    return


@app.cell
def _(df, pl):
    df_industry = (df
        .select(["name", "city", "launch_year", "growth_stage", "industry"])
        .unique(subset=["name", "city", "launch_year", "growth_stage", "industry"], keep="first")
        .filter(pl.col("industry").is_not_null() & pl.col("launch_year").is_not_null())
            )
    df_industry
    return (df_industry,)


@app.cell
def _(df_industry, pl):
    count_industry_launch_year = (df_industry
                                 .group_by(["launch_year", "industry"])
                                 .agg(pl.len().alias("count"))
                                 )
    count_industry_launch_year
    return (count_industry_launch_year,)


@app.cell
def _(count_industry_launch_year):
    count_industry_launch_year.select("industry").unique()
    return


@app.cell
def _(count_industry_launch_year, pl):
    # pick top industries
    top_n = 6

    tops_pl = (count_industry_launch_year
              .group_by("industry")
              .agg(pl.col("count").sum().alias("total"))
              .sort("total", descending = True)
               .head(top_n)
               #.select("industry")
              )
    return top_n, tops_pl


@app.cell
def _(tops_pl):
    top_industries = tops_pl.select("industry").to_series().to_list()
    return (top_industries,)


@app.cell
def _(top_industries):
    top_industries
    return


@app.cell
def _(count_industry_launch_year, pl, top_industries):
    industry_trends_pl = (count_industry_launch_year
                         .filter(pl.col("industry").is_in(top_industries))
                          .sort("launch_year")
                         )
    return (industry_trends_pl,)


@app.cell
def _(industry_trends_pl):
    industry_trends_pd = industry_trends_pl.to_pandas()
    return (industry_trends_pd,)


@app.cell
def _(industry_trends_pd, plt, sns, top_n):
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=industry_trends_pd, x="launch_year", y="count", hue="industry", marker="o")
    plt.title(f"Top {top_n} Industries by Launch Year")
    plt.xlabel("Launch Year")
    plt.ylabel("Company Count")
    plt.legend(title="Industry")
    plt.tight_layout()
    plt.show()
    return


@app.cell
def _(mo):
    mo.md(r"""### AI vs non-AI over time""")
    return


@app.cell
def _(df, pl):
    ai_trend = (df
               .select(["launch_year", "is_ai_data"])
               .filter(pl.col("launch_year").is_not_null() & pl.col("is_ai_data").is_not_null())
                .group_by(["launch_year", "is_ai_data"])
                .agg(pl.len().alias("counts"))
                .sort(by = "launch_year")
               )
    ai_trend
    return (ai_trend,)


@app.cell
def _(ai_trend):
    ai_trend_pd = ai_trend.to_pandas()
    return (ai_trend_pd,)


@app.cell
def _(ai_trend_pd, plt, sns):
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=ai_trend_pd, x="launch_year", y="counts", hue="is_ai_data", marker="o")
    plt.title("AI vs Non AI Companies Over Time")
    plt.xlabel("Launch Year")
    plt.ylabel("Count")
    plt.legend(title="is_ai_data")
    plt.tight_layout()
    plt.show()

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
