import logging


def get_logger(name: str, log_level="INFO") -> logging.Logger:
    """Get logger.

    Args:
        name (str): Name of logger.
        log_level (str, optional): Log level. Defaults to "INFO".

    Returns:
        logging.Logger: Logger.
    """

    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        level=log_level,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(name)
