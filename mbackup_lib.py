import os
import sys
import shutil
import glob
import re
# import dateutil.parser as dparser
import dateparser
import pandas as pd
import logging
from distutils.dir_util import copy_tree, DistutilsFileError
import config as cfg

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


def to_mb(size_bytes):
	return size_bytes / float(1 << 20)


def to_gb(size_bytes):
	return size_bytes / float(1 << 30)


def format_size(size, nums=2):
	strsize = str(round(size, nums)) if abs(size) >= 1 else f"{size:.{nums}g}"
	return strsize.replace('.', ',')


def backup_directory_simple(srcdir, dstdir):
	check_dir(dstdir)
	try:
		logger.info(f"Copy tree in '{srcdir}' to '{dstdir}'")
		copy_tree(srcdir, dstdir, update=1)
	except DistutilsFileError as e:
		logger.error(f"Error while copying tree in '{srcdir}' to '{dstdir}': {e}")
		return False
	return dstdir


def get_files_info(src, pattern="*"):
	path = src if pattern is None or src is None else os.path.join(src, pattern)
	files = pd.DataFrame(columns=['path', 'date', 'date_diff', 'size'])
	if path is not None:
		filelist = [f for f in glob.glob(path) if os.path.isfile(f)]
		if len(filelist) > 0:
			files = pd.DataFrame({'path': filelist})
			# files['date'], files['date_diff'] = zip(*files.apply(lambda row: parse_file_date(row['path']), axis=1))
			files[['date', 'date_diff']] = files.apply(lambda row: parse_file_date(row['path']), axis=1, result_type="expand")
			files['size'] = files.apply(lambda row: os.path.getsize(row['path']), axis=1)

	return files


def parse_file_date(name):
	match = re.findall(cfg.filename_date_pattern, name)
	if match:
		# dt = dparser.parse(match.group(), fuzzy_with_tokens=True)
		dt = [dateparser.parse(m, date_formats=cfg.filename_date_formats) for m in match]
		if len(dt) == 1:
			dt.append(None)
		return dt
	else:
		logger.debug(f"Error parse datetime in '{name}'")
		return [None] * 2


def restrict_data_file(rootpath):
	"""
	Deprecate function
	Move files from digits dirs to parent dir
	:param rootpath: search here and move to
	"""
	date_dirname_len = 11
	pattern = f"{[0-9] * date_dirname_len}*"
	dtdirs = glob.glob(os.path.join(rootpath, pattern, '**'), recursive=True)
	for d in dtdirs:
		if os.path.isfile(d):
			relpath = os.path.relpath(os.path.relpath(d, rootpath)[date_dirname_len:], os.path.sep)
			newpath = os.path.join(rootpath, relpath)
			if not os.path.isdir(os.path.dirname(newpath)):
				os.makedirs(os.path.dirname(newpath))
			shutil.move(d, newpath)
	for d in glob.glob(os.path.join(rootpath, pattern)):
		shutil.rmtree(d)
	print(len(dtdirs))
