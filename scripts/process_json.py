import marimo

__generated_with = "0.14.16"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from config import RAW_DIR, PROCESSED_DIR
    return PROCESSED_DIR, RAW_DIR, mo, pl


@app.cell
def _(RAW_DIR):
    file_path = RAW_DIR / "startup_berlin_28072025.json"
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
    small_df = df.select(["name", "hq_locations", "launch_year", "type", "industries", "technologies", "tech_stack", "service_industries", "corporate_industries", "is_ai_data", "growth_stage", "investors", "year_became_unicorn", "income_streams", "latest_valuation_enhanced", "sdgs", "ipo_round", "innovation_corporate_rank", "investments"])
    small_df.head()
    return (small_df,)


@app.cell
def _(pl, small_df):
    # explode location
    df_location = (
        small_df.explode(["hq_locations"])
          .with_columns([
              pl.col("hq_locations").struct.field("city").struct.field("name").alias("city"),
        ]) 
          .drop(["hq_locations"])  # drop original struct column
    )

    df_location.head(20)
    return (df_location,)


@app.cell
def _(df_location, pl):
    # explode industries
    df_ind = (df_location
        .explode("industries")
        .with_columns([
            pl.col("industries").struct.field("name").alias("industry")
        ])
        .drop("industries")
              )

    df_ind.head(20)
    return (df_ind,)


@app.cell
def _(df_ind, pl):
    # explode technologies
    df_tech = (df_ind
               .explode("technologies")
               .with_columns([
                   pl.col("technologies").struct.field("name").alias("technology")
               ])
               .drop("technologies")
    )

    df_tech.head(20)
    return (df_tech,)


@app.cell
def _(df_tech, pl):
    # explode tech_stack
    df_tech_stack = (df_tech
              .explode("tech_stack")
              .with_columns([
                  pl.col("tech_stack").struct.field("name").alias("tech_stack_name")
              ])
               .drop("tech_stack")
              )
    df_tech_stack.head(10)
    return (df_tech_stack,)


@app.cell
def _(df_tech_stack, pl):
    # explode service_industries
    df_service = (df_tech_stack
              .explode("service_industries")
              .with_columns([
                  pl.col("service_industries").struct.field("name").alias("service_industries_name")
              ])

              )

    # explode corporate_industries
    df_corporate = (df_service
              .explode("corporate_industries")
               .with_columns([
                   pl.col("corporate_industries").struct.field("name").alias("corporate_industries_name")
               ])
              )
    df_corporate.head(20)
    return (df_corporate,)


@app.cell
def _(df_corporate, pl):
    # explode investors
    df_investors = (
        df_corporate
        # grab the list-of-investor-dicts → new column `inv`
        .with_columns(pl.col("investors").struct.field("items").alias("inv"))
        .explode("inv")
        .with_columns([
            pl.col("inv").struct.field("type").alias("investor_type"),
            pl.col("inv").struct.field("entity_type").alias("investor_entity_type"),
            pl.col("inv").struct.field("name").alias("investor_name"),
            pl.col("inv").struct.field("exited").alias("investor_exited"),
            pl.col("inv").struct.field("lead").alias("investor_lead")

        ])
        .drop(["inv", "investors"])
    )
    df_investors.head(10)
    return (df_investors,)


@app.cell
def _(df_investors, pl):
    # explode income streams
    df_income_streams = (df_investors
                         .explode("income_streams")
                          .with_columns([
                              pl.col("income_streams").struct.field("name").alias("income_stream_name")
                          ])
                          .drop("income_streams")
                         )
    df_income_streams.head(10)
    return (df_income_streams,)


@app.cell
def _(df_income_streams):
    # explode latest_valuation_enhanced

    valuation_cols = ["year", "month", 
        "source", "source_round",
        "valuation", "valuation_min", "valuation_max", "valuation_currency",
        "market_cap","net_debt"]

    df_valuation = (df_income_streams
                   .unnest("latest_valuation_enhanced")
        .rename({c: f"valuation_{c}" for c in valuation_cols})
                   )

    df_valuation.head(20)
    return (df_valuation,)


@app.cell
def _(df_valuation):
    df_flat = (df_valuation
              .drop(["sdgs", "ipo_round", "investments"]))

    df_flat.head(20)
    return (df_flat,)


@app.cell
def _(PROCESSED_DIR, df_flat):
    df_flat.write_parquet(PROCESSED_DIR / "processed_startup_berlin_28072025.parquet",
                         compression = "zstd",
                         compression_level = 3)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
