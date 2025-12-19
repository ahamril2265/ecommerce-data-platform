def assert_not_null(df, columns):
    for col in columns:
        if df[col].isnull().any():
            raise ValueError(f"Nulls found in required column: {col}")
