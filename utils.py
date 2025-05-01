def log_message(message: str, level: int, config_verbosity: int):
    """Prints a message if its level is less than or equal to the configured verbosity.

    Args:
        message (str): The message to print.
        level (int): The verbosity level required to print this message (1=info, 2=debug).
        config_verbosity (int): The verbosity level set in the config (0=silent, 1=info, 2=debug).
    """
    if config_verbosity >= level:
        print(message)