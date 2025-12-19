def hard_checks(df, rules: dict):
    for col, rule in rules.items():
        if rule == "not_null" and df[col].isnull().any():
            raise ValueError(f"Hard check failed: {col} contains nulls")

        if rule == "positive" and (df[col] <= 0).any():
            raise ValueError(f"Hard check failed: {col} contains non-positive values")


def soft_checks(df, expectations: dict, logger):
    for metric, (min_val, max_val) in expectations.items():
        value = df[metric].sum()
        if not (min_val <= value <= max_val):
            logger.warning(
                f"Soft check warning: {metric}={value} outside range {min_val}-{max_val}"
            )
