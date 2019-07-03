import glob
import re
import shutil
from datetime import datetime
import time
import tarfile
# import dateutil.parser as dparser
import dateparser
import pandas as pd
import humanize as hu
from multiprocessing.dummy import Pool

from logger import set_logger
from mbackup_lib import *
from t7z_lib import *
from DataSaverClass import DataSaver

logger = set_logger('__main__', os.path.basename(__file__))
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', 120)

checked_archs = DataSaver(cfg.path_checked_archs, ['filename', 'filemtime'], parse_dates=['filemtime'])
backup_stats = DataSaver(cfg.path_backup_stats, ['backups', 'from', 'after'])


def to_mb(size_bytes):
	return round(size_bytes / float(1 << 20), 2)


def to_gb(size_bytes):
	return round(size_bytes / float(1 << 30), 2)


def logging_sizes(type_str, size, count):
	logger.info(f"{type_str} size: {hu.naturalsize(size, gnu=True)} ({to_gb(size)}), count: {count}")


def check_tar_extract_member_file(need_check, path, member):
	if not need_check:
		return True
	file = os.path.join(path, member.name)
	if os.path.isfile(file) and os.path.getmtime(file) == member.mtime:
		return False
	return True


def check_checked_archs(data):
	for i, d in data.iterrows():
		if not os.path.isfile(d.filename) or not d.filemtime == pd.to_datetime(os.path.getmtime(d.filename), unit='s'):
			data = data.drop([i])
	return data


def test_arch(file):
	file_mtime = pd.to_datetime(os.path.getmtime(file), unit='s')
	if any(checked_archs.data.filename == os.path.normpath(file)) and any(checked_archs.data.filemtime == file_mtime):
		return True
	if test_7z(file):
		checked_archs.add([os.path.normpath(file), file_mtime])
		return True
	else:
		logger.warning(f"Archive '{file}' test failed. Will be delete")
		if os.path.isfile(file):
			try:
				os.remove(file)
			except OSError as e:
				exit_log(f"Not Deleting file: {e}")
		return False


def subpath_proc(subs, srcroot, ptype):
	check_dir(srcroot)
	logger.info(f"Proc subs #{len(subs)} in '{srcroot}'")
	for subpath in subs:
		src = os.path.join(srcroot, subpath)
		logger.info(f"Proc sub '{subpath}'")
		temp_path = os.path.join(cfg.temp_dir, subpath)
		dst_path = os.path.join(cfg.arch_path_recompress, subpath)
		if ptype is "data":
			fulldata = filter_data_files(get_files_info(src, pattern="*.tar.gz"))
			diffdata = filter_data_diff_files(get_files_info(src, pattern="*.tar.bz2"), fulldata.date.to_list())
			basename = os.path.basename(fulldata['path'].all()).split(".")[0]
			period = get_date_period_files(fulldata)
			dst_path = os.path.join(dst_path, period)
			print(fulldata)

			total_size = fulldata['size'].sum() + diffdata['size'].sum()
			logging_sizes(f"Fulldata {{{subpath}}}", fulldata['size'].sum(), len(fulldata))
			logging_sizes(f"Diffdata {{{subpath}}}", diffdata['size'].sum(), len(diffdata))
			logging_sizes(f"Total {{{subpath}}}", total_size, len(fulldata) + len(diffdata))
			full_temp_path = os.path.join(temp_path, "full")
			data_proc(fulldata, full_temp_path, dst_path)

			for date_diff, dd in group_data_diff_files(diffdata):
				print(dd)
				diff_arch_name = f"{basename}.{date_diff.strftime(cfg.filename_date_formats[0])}_diffs.7z"
				diff_arch_path = os.path.join(dst_path, diff_arch_name)
				diff_temp_path = os.path.join(temp_path, date_diff.strftime(cfg.filename_date_formats[0]))
				if not check_arch_exist(diff_arch_path) or not test_arch(diff_arch_path):
					data_diff_proc(diffdata, diff_temp_path, diff_arch_path)
		else:
			data = filter_single_files(get_files_info(src))
			period = get_date_period_files(data)
			dst_path = os.path.join(dst_path, period)
			# print(data)
			logging_sizes(f"Total {{{subpath}}}", data['size'].sum(), len(data))
			single_proc(data, temp_path, dst_path)


def get_files_info(src, pattern="*"):
	files = pd.DataFrame({'path': glob.glob(os.path.join(src, pattern))})
	# files['date'], files['date_diff'] = zip(*files.apply(lambda row: parse_file_date(row['path']), axis=1))
	files[['date', 'date_diff']] = files.apply(lambda row: parse_file_date(row['path']), axis=1, result_type="expand")
	files['size'] = files.apply(lambda row: os.path.getsize(row['path']), axis=1)

	return files


def group_data_diff_files(files):
	grouped_files = [[date_diff, part] for date_diff, part in files.groupby('date_diff')]
	return grouped_files


def filter_single_files(files):
	return files[(files.date.dt.month < datetime.today().month) | (files.date.dt.year < datetime.today().year)]


def filter_data_files(files):
	return files[files.date != files.date.max()]


def filter_data_diff_files(files, fulldates=None):
	fulldates = [] if fulldates is None else fulldates
	return files[files.date_diff.isin(fulldates)]


def get_date_period_files(files):
	min_date = files.date.min()
	max_date = files.date.max()
	if min_date.year == max_date.year:
		return f"{min_date:%Y}_{min_date:%m}-{max_date:%m}"
	else:
		return f"{min_date:%Y}_{min_date:%m}-{max_date:%Y}_{max_date:%m}"


def parse_file_date(name):
	match = re.findall(cfg.filename_date_pattern, name)
	if match:
		# dt = dparser.parse(match.group(), fuzzy_with_tokens=True)
		dt = [dateparser.parse(m, date_formats=cfg.filename_date_formats) for m in match]
		if len(dt) == 1:
			dt.append(None)
		return dt
	else:
		logger.warning(f"Error parse datetime in '{name}'")
		return [None] * 2


def data_proc(files, temp, dst):
	for i, f in files.iterrows():
		epath = os.path.join(temp, os.path.basename(f.path))
		# extract_tar_incremental_data(f.path, epath)


def data_diff_proc(files, temp, dst):
	tasks = [(f.path, os.path.join(temp, os.path.basename(f.path))) for i, f in files.iterrows()]
	p = Pool(cfg.extracting_pool_size)
	p.starmap(extract_tar_incremental_data, tasks)
	compress_7z_data(dst, temp)
	# shutil.rmtree(temp)


def single_proc(files, temp, dst):
	for i, f in files.iterrows():
		# extract_7z_single(f.path, dst)
		pass


def extract_tar_incremental_data(file, path):
	tar = tarfile.open(file)
	need_check = True if os.path.isdir(path) else False
	for member in tar.getmembers():
		if member.isfile():
			member.name = '/'.join(member.name.split('/')[2:])
			if check_tar_extract_member_file(need_check, path, member):
				tar.extract(member, path)


def restrict_data_file(rootpath):
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


def main():
	# Проверяем наличие необходимых директорий
	for cd in [cfg.arch_path_recompress, cfg.temp_dir]:
		check_dir(cd)

	checked_archs.data = check_checked_archs(checked_archs.data)
	checked_archs.save()

	pool = Pool()
	pool.starmap(subpath_proc, [
		(cfg.sub_path_data, cfg.arch_path_data, "data"),
		(cfg.sub_path_single, cfg.arch_path_single, "single")])
	checked_archs.save()
	backup_stats.save()


if __name__ == "__main__":
	main()
