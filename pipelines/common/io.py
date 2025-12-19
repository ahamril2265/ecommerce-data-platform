from pathlib import Path
import pandas as pd

def read_parquet(path):
    return pd.read_parquet(path)

def write_parquet(df, path, partition_col=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if partition_col:
        for value, part_df in df.groupby(partition_col):
            part_path = path / f"{partition_col}={value}"
            part_path.mkdir(parents=True, exist_ok=True)
            part_df.drop(columns=[partition_col]).to_parquet(
                part_path / "data.parquet",
                index=False
            )
    else:
        df.to_parquet(path, index=False)
