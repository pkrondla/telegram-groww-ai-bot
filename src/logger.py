import logging
import sys
from .config import Config

def setup_logger():
    """
    Industrial-standard dual-output logger setup.
    Outputs to both the console and a persistent 'bot_activity.log' file.
    """
    
    # Create a custom logger
    logger = logging.getLogger("TradingBot")
    logger.setLevel(Config.LOG_LEVEL)
    logger.propagate = False

    # Prevent duplicate handlers when module is re-imported/reloaded
    if logger.handlers:
        return logger

    import os
    from datetime import datetime
    
    # Create logs directory if missing
    if not os.path.exists("logs"):
        os.makedirs("logs")
        
    log_filename = f"logs/trading_{datetime.now().strftime('%Y-%m-%d')}.log"

    # Force stdout to handle UTF-8 if possible (Fix for Windows Unicode Errors)
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    
    # Create handlers
    c_handler = logging.StreamHandler(sys.stdout)
    f_handler = logging.FileHandler(log_filename, encoding='utf-8')
    
    c_handler.setLevel(Config.LOG_LEVEL)
    f_handler.setLevel(Config.LOG_LEVEL)

    # Create formatters and add it to handlers
    # Example: 2026-04-03 23:25:01 [INFO] :: Bot started...
    log_format = logging.Formatter('%(asctime)s [%(levelname)s] :: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    c_handler.setFormatter(log_format)
    f_handler.setFormatter(log_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    return logger

# Single instance of the logger to be used across all modules
logger = setup_logger()
