from datetime import datetime
import tarfile
import humanize as hu
from multiprocessing.dummy import Pool

from logger import set_logger
from mbackup_lib import *
from t7z_lib import *
from DataSaver import DataSaver
from TaskManager import TaskManager

logger = set_logger('__main__', os.path.basename(__file__))
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', 120)

checked_archs = DataSaver(cfg.path_checked_archs, ['filename', 'filemtime'], parse_dates=['filemtime'])
backup_stats = DataSaver(cfg.path_backup_stats, ['backups', 'from', 'after'])


def logging_sizes(type_str, sub, size, count):
	formsize = format_size(to_gb(size))
	logger.info(f"{type_str} {{{sub}}} size: {hu.naturalsize(size, gnu=True)} ({formsize}), count: {count}")
	if type_str == "Total":
		backup_stats.add([sub, formsize, 0])
	if type_str == "Result":
		backup_stats.data.loc[(backup_stats.data.backups == sub), 'after'] = formsize


def check_checked_archs(data):
	for i, d in data.iterrows():
		if not os.path.isfile(d.filename) or not d.filemtime == pd.to_datetime(os.path.getmtime(d.filename), unit='s'):
			data = data.drop([i])
	return data


def extract_tar_incremental_data(file, path):
	tar = tarfile.open(file)
	need_check = True if os.path.isdir(path) else False
	for member in tar.getmembers():
		if member.isfile():
			member.name = '/'.join(member.name.split('/')[2:])
			if check_tar_extract_member_file(need_check, path, member):
				tar.extract(member, path)


def check_tar_extract_member_file(need_check, path, member):
	if not need_check:
		return True
	file = os.path.join(path, member.name)
	if os.path.isfile(file) and os.path.getmtime(file) == member.mtime:
		return False
	return True


def test_arch(file):
	file_mtime = pd.to_datetime(os.path.getmtime(file), unit='s')
	if any((checked_archs.data.filename == os.path.normpath(file)) & (checked_archs.data.filemtime == file_mtime)):
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


def subpath_proc(subs, ptype):
	logger.info(f"Proc subs #{len(subs)} in '{ptype}'")
	for subpath, srcroot in subs.items():
		src = os.path.join(srcroot, subpath)
		check_dir(src)
		logger.info(f"Proc sub '{subpath}'")
		temp_path = os.path.join(cfg.temp_dir, subpath)
		dst_path = os.path.join(cfg.arch_path_recompress, subpath)
		result_files = get_files_info(None)
		if ptype is "data":
			fulldata = filter_data_files(get_files_info(src, pattern="*.tar.gz"))
			diffdata = filter_data_diff_files(get_files_info(src, pattern="*.tar.bz2"), fulldata.date.to_list())
			basename = os.path.basename(fulldata['path'].all()).split(".")[0]
			period = get_date_period_files(fulldata)
			dst_path = os.path.join(dst_path, period)
			print(fulldata)

			total_size = fulldata['size'].sum() + diffdata['size'].sum()
			logging_sizes("Fulldata", subpath, fulldata['size'].sum(), len(fulldata))
			logging_sizes("Diffdata", subpath, diffdata['size'].sum(), len(diffdata))
			logging_sizes("Total", subpath, total_size, len(fulldata) + len(diffdata))
			full_temp_path = os.path.join(temp_path, "full")
			data_proc(fulldata, full_temp_path, dst_path)

			for date_diff, dd in group_data_diff_files(diffdata):
				# print(dd)
				diff_arch_name = f"{basename}.{date_diff.strftime(cfg.filename_date_formats[0])}_diffs.7z"
				diff_arch_path = os.path.join(dst_path, diff_arch_name)
				diff_temp_path = os.path.join(temp_path, date_diff.strftime(cfg.filename_date_formats[0]))
				if not check_arch_exist(diff_arch_path) or not test_arch(diff_arch_path):
					data_diff_proc(diffdata, diff_temp_path, diff_arch_path)
			result_files = get_files_info(dst_path)

		else:
			data = filter_single_files(get_files_info(src))
			print(data)
			logging_sizes("Total", subpath, data['size'].sum(), len(data))
			if not data.empty:
				period = get_date_period_files(data)
				single_temp_path = os.path.join(temp_path, period)
				single_arch_name = f"{period}.7z"
				dst_path = os.path.join(dst_path, f"{data.date.min():%Y}", single_arch_name)

				single_proc(data, single_temp_path, dst_path)

				result_files = get_files_info(dst_path, pattern=None)

		logging_sizes("Result", subpath, result_files['size'].sum(), len(result_files))


def group_data_diff_files(files):
	grouped_files = [[date_diff, part] for date_diff, part in files.groupby('date_diff')]
	return grouped_files


def filter_single_files(files):
	if files.empty:
		return files
	return files[(files.date.dt.month < datetime.today().month) | (files.date.dt.year < datetime.today().year)]


def filter_data_files(files):
	if files.empty:
		return files
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


def data_proc(files, temp, dst):
	for i, f in files.iterrows():
		epath = os.path.join(temp, os.path.basename(f.path))
		# extract_tar_incremental_data(f.path, epath)


def data_diff_proc(files, temp, dst):
	tasks = [(f.path, os.path.join(temp, os.path.basename(f.path))) for i, f in files.iterrows()]
	p = Pool(cfg.extracting_pool_size)
	p.starmap(extract_tar_incremental_data, tasks)
	compress_7z_data(dst, temp)
	shutil.rmtree(temp)


def single_proc(files, temp, dst):
	tasks = [(f.path, temp) for i, f in files.iterrows()]
	# p = Pool(2)
	# p.starmap(extract_7z_single, tasks)
	for i, f in files.iterrows():
		extract_and_compress_7z_single(f.path, dst)
	# shutil.rmtree(temp)


def main():
	# Проверяем наличие необходимых директорий
	for cd in [cfg.arch_path_recompress, cfg.temp_dir]:
		check_dir(cd)

	checked_archs.data = check_checked_archs(checked_archs.data)
	checked_archs.save()
	# Распараллеливаем процесс на 2 потока
	# pool = Pool()
	# pool.starmap(subpath_proc, [
	# 	(cfg.sub_path_data, "data"),
	# 	(cfg.sub_path_single, "single")])
	tm = TaskManager()
	tm.initFiles()
	checked_archs.save()
	backup_stats.save()


if __name__ == "__main__":
	main()
