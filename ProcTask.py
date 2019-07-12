from mbackup_lib import *
import logging

logger = logging.getLogger(f"__main__.{__name__}")


class ProcTask(object):
	storage = ''
	sub = ''
	ztype = ''
	pattern = '*'

	files = None
	result_files = None
	files_info = {}

	manager = None

	def __init__(self, **kwargs):
		attrs = [x for x in dir(self) if not callable(x) and not(x.startswith('__') and x.endswith('__'))]
		self.__dict__.update((k, v) for k, v in kwargs.items() if k in attrs)

		self.store_path = os.path.normpath(os.path.join(self.storage, self.sub))
		self.temp_path = os.path.normpath(os.path.join(cfg.temp_dir, self.sub))
		self.reco_path = os.path.normpath(os.path.join(cfg.arch_path_recompress, self.sub))

	def init_files(self):
		logger.debug(f"Init files from '{self.store_path}' by '{self.pattern}'")
		check_dir(self.store_path)
		self.result_files = get_files_info(None)
		self.files = self.filter_files(get_files_info(self.store_path, self.pattern))

	def init_files_info(self):
		name = os.path.basename(self.files['path'].get(self.files.first_valid_index(), ''))
		matchmane = re.search(r"(^[a-z_-]+)(\.|\d)", name, re.IGNORECASE)
		self.files_info['basename'] = matchmane.group(1) if matchmane else self.sub.split('/')[-1]
		self.files_info['period'] = self.get_date_period_files(self.files)
		self.files_info['size'] = self.files['size'].sum()
		self.files_info['arch_name'] = os.path.basename(self.files['path'].get(self.files.first_valid_index(), ''))

	@staticmethod
	def filter_files(files):
		return files

	@staticmethod
	def get_date_period_files(files):
		if files.empty:
			return ''
		min_date = files.date.min()
		max_date = files.date.max()
		if min_date.year == max_date.year:
			return f"{min_date:%Y}_{min_date:%m}-{max_date:%m}"
		else:
			return f"{min_date:%Y}_{min_date:%m}-{max_date:%Y}_{max_date:%m}"

	def get_files_date(self):
		return self.files.date.to_list() if self.files is not None else []

	def check_files(self):
		return not self.files.empty

	def get_ext(self):
		return self.pattern.replace('*', '')
