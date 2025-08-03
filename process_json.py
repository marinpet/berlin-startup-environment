import marimo

__generated_with = "0.14.16"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _():
    file_path = "data/startup_berlin_28072025.json"
    return (file_path,)


@app.cell
def _(file_path, pl):
    raw = pl.read_json(file_path)
    return (raw,)


@app.cell
def _(raw):
    df = (
        raw.select("items")
        .explode("items")
        .unnest("items")
    )
    return (df,)


@app.cell
def _(df):
    df.shape
    return


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _(df):
    df
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Interesting columns:

    * investors
    * year_became_unicorn
    * launch_year
    * tech_stack
    * technologies
    * lists?
    * industries
    * service_industries
    * hq_locations
    * income_streams
    * latest_valuation_enhanced
    * sdgs?
    * ipo_round?
    * corporate_industries
    * name
    * innovation_corporate_rank
    * is_ai_data
    * growth_stage
    * investments
    * kpi_summary
    * type
    """
    )
    return


@app.cell
def _(df):
    small_df = df.select(["name", "hq_locations", "type", "investors", "year_became_unicorn", "launch_year", "tech_stack", "technologies", "lists", "industries", "service_industries", "income_streams", "latest_valuation_enhanced", "sdgs", "ipo_round", "corporate_industries", "innovation_corporate_rank", "is_ai_data", "growth_stage", "investments", "kpi_summary"])
    small_df.head()
    return (small_df,)


@app.cell
def _(pl, small_df):
    # explode location
    df_flat = (
        small_df.explode(["hq_locations"])
          .with_columns([
              pl.col("hq_locations").struct.field("city").struct.field("name").alias("city")
              # add more fields as needed
          ])
          .drop("hq_locations")  # drop original struct column
    )

    # explode industries
    df_flat = (df_flat
        .explode("industries")
        .with_columns([
            pl.col("industries").struct.field("name").alias("industry")
        ])
        .drop("industries")
              )
    # explode technologies
    df_flat = (df_flat
               .explode("technologies")
               .with_columns([
                   pl.col("technologies").struct.field("name").alias("technology")
               ])
               .drop("technologies")
    )

    # explode tech_stack
    df_flat = (df_flat
              .explode("tech_stack")
              .with_columns([
                  pl.col("tech_stack").struct.field("name").alias("tech_stack_name")
              ])
               .drop("tech_stack")
              )

    # explode service_industries
    df_flat = (df_flat
              .explode("service_industries")
              .with_columns([
                  pl.col("service_industries").struct.field("name").alias("service_industry")
              ])

              )

    df_flat = (
        df_flat.select("name", "city", "launch_year", "type", "industry", "technology", "tech_stack_name", "service_industry", "investors", "year_became_unicorn", "lists", "income_streams", "latest_valuation_enhanced", "sdgs", "ipo_round", "corporate_industries", "innovation_corporate_rank", "is_ai_data", "growth_stage", "investments", "kpi_summary"))


    df_flat.head()

    return (df_flat,)


@app.cell
def _(df_flat):
    df_flat.select("service_industry").unique()
    return


@app.cell
def _(df):
    df.select("service_industries").unique()
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("service_industries").is_not_null())
    return


@app.cell
def _(df, pl):
    (df
        .filter(pl.col("service_industries").apply(lambda x: len(x) if x is not None else 0) > 0)
        .select("name", "service_industries")
    )
    return


@app.cell
def _(df, pl):
    (df
        .filter(pl.col("name") == "Elestia")
        .select("name", "service_industries")
    )
    return


@app.cell
def _(df):
    df.schema.get("service_industries")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
