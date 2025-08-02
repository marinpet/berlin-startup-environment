import marimo

__generated_with = "0.14.13"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return (pl,)


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
def _():
    return


if __name__ == "__main__":
    app.run()
