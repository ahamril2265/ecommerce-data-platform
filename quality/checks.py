def validate_dataframe(df, schema):
    schema_cols = {col["name"] for col in schema["columns"]}
    df_cols = set(df.columns)

    missing = schema_cols - df_cols
    extra = df_cols - schema_cols

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if extra:
        raise ValueError(f"Unexpected columns: {extra}")
