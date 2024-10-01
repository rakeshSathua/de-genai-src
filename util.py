# log_util.py

import logging
import os


class LogUtil:
    @staticmethod
    def setup_logging(log_file="app.log", level=logging.INFO):
        """
        Set up logging to file and console.

        Args:
            log_file (str): Path to the log file where logs should be saved.
            level (logging.Level): Logging level, e.g., logging.INFO, logging.DEBUG.
        """
        log_format = "%(asctime)s %(levelname)s: %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        log_dir = "logs"

        # Create logs directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Set up file handler with specified log file
        file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
        file_handler.setFormatter(
            logging.Formatter(fmt=log_format, datefmt=date_format)
        )

        # Set up stream handler for console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(fmt=log_format))

        # Get the root logger and set the level
        logger = logging.getLogger()
        logger.setLevel(level)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # Log that the logger has been set up
        logger.info(
            "Logging is set up. Log file is at %s", os.path.join(log_dir, log_file)
        )
