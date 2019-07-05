import os
import logging
import subprocess
import config as cfg
from mbackup_lib import check_arch_exist

logger = logging.getLogger(f"__main__.{__name__}")


def extract_7z_single(file, path):
	command = [cfg.path_7z, "x", "-aoa", file]
	run_7z_subproc(command, path)


def extract_and_compress_7z_single(file, fileto):
	filename = os.path.splitext(os.path.basename(file))[0]
	command = [cfg.path_7z, "e", "-so", file, "|", cfg.path_7z, "a", "-t7z", "-mx9", "-ssw", f"-si{filename}" "-up1q1r2w2", fileto]
	run_7z_subproc(command, os.path.dirname(fileto))


def extract_7z_tar_data(file, path):
	command = [cfg.path_7z, "e", "-so", file, "|", cfg.path_7z, "x", "-aoa", "-si", "-ttar"]
	run_7z_subproc(command, path)


def compress_7z_data(file, path):
	if not check_arch_exist(file):
		command = [cfg.path_7z, "a", "-r", "-t7z", "-mx9", "-ssw", file, "*"]
		run_7z_subproc(command, path)


def test_7z(file):
	if os.path.isfile(file):
		command = [cfg.path_7z, "t", file]
		return run_7z_subproc(command, os.path.dirname(file))
	else:
		return False


def run_7z_subproc(arglist, path):
	if not os.path.exists(path):
		os.makedirs(path)
	# noinspection PyBroadException
	try:
		logger.debug(f"Try running cmd: '{' '.join(arglist)}' in directory {path}")
		sp = subprocess.Popen(
			args=arglist,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			cwd=path,
			bufsize=1,
			shell=True,
			universal_newlines=True)
	except Exception:
		logger.error("Error while running 7z subprocess.")
		return False
	# wait for process to terminate, get stdout and stderr
	stdout, stderr = sp.communicate()
	if stdout:
		logger.debug(f"\n>>> 7z subprocess STDOUT START <<<{stdout}>>> 7zip subprocess STDOUT END <<<")
	if stderr:
		logger.debug(f"7z STDERR:\n{stderr}")
		return False
	return True
