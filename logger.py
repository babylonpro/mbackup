import logging
import logging.handlers


def set_logger(name, fname, level=logging.DEBUG):
	logger = logging.getLogger(name)
	formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	fname = f"{fname}.log"
	fhandler = logging.handlers.TimedRotatingFileHandler(fname, when="D", backupCount=16, encoding="utf-8")
	fhandler.setFormatter(formatter)
	fhandler.setLevel(logging.INFO)

	chandler = logging.StreamHandler()
	chandler.setFormatter(formatter)
	chandler.setLevel(level)

	logger.addHandler(chandler)
	logger.addHandler(fhandler)
	logger.setLevel(level)
	return logger
