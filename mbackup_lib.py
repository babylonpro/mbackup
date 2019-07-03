import os
import sys
import logging
from distutils.dir_util import copy_tree, DistutilsFileError

logger = logging.getLogger(f"__main__.{__name__}")


def exit_log(exitstring):
	logger.critical(exitstring)
	sys.exit()


def check_dir(path):
	if not os.path.exists(path):
		exit_log(f"Directory '{path}' not exist")


def check_arch_exist(file):
	if os.path.isfile(file):
		logger.warning(f"Archive '{file}' already exist")
		return True
	return False


def backup_directory_simple(srcdir, dstdir):
	try:
		logger.info(f"Copy tree in '{srcdir}' to '{dstdir}'")
		copy_tree(srcdir, dstdir, update=1)
	except DistutilsFileError as e:
		logger.error(f"Error while copying tree in '{srcdir}' to '{dstdir}': {e}")
		return False
	return dstdir
