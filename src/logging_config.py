import logging

# Define ANSI color codes for different log levels
LOG_COLORS = {
    'DEBUG': "\033[36m",   # Cyan
    'INFO': "\033[32m",    # Green
    'WARNING': "\033[33m", # Yellow
    'ERROR': "\033[31m",   # Red
    'CRITICAL': "\033[35m" # Magenta
}
RESET_COLOR = "\033[0m"  # Reset to default color

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        log_color = LOG_COLORS.get(record.levelname, RESET_COLOR)
        record.msg = f"{log_color}{record.msg}{RESET_COLOR}"
        return super().format(record)

def setup_logging():
    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set the lowest level you want to capture

    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create formatter with colors
    formatter = ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to the handler
    ch.setFormatter(formatter)

    # Add handler to the logger
    logger.addHandler(ch)

    return logger

# Initialize the logger
logger = setup_logging()
