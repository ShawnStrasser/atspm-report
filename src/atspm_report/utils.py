import pandas as pd


def log_message(message: str, level: int, config_verbosity: int):
    """Prints a message if its level is less than or equal to the configured verbosity.

    Args:
        message (str): The message to print.
        level (int): The verbosity level required to print this message (1=info, 2=debug).
        config_verbosity (int): The verbosity level set in the config (0=silent, 1=info, 2=debug).
    """
    if config_verbosity >= level:
        print(message)

def text_equals(values: pd.Series, text: str) -> pd.Series:
    """values.astype(str) == text, without converting a column that already holds text.

    A timeline runs to tens of millions of rows, where the conversion costs seconds.
    """
    if pd.api.types.is_string_dtype(values):
        return values == text
    return values.astype(str) == text
