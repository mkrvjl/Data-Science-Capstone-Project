import pandas as pd


def merge_columns(
    df: pd.DataFrame,
    col_to_mask: str,
    other_col: str,
    str_to_match: str,
    new_col: str,
) -> pd.DataFrame:
    df[new_col] = pd.DataFrame(
        df[col_to_mask].mask(df[col_to_mask].str.match(str_to_match), df[other_col])
    )

    df[new_col] = df[new_col].str.upper()

    return df
