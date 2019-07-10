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

	def __init__(self, **kwargs):
		attrs = [x for x in dir(self) if not callable(x) and not(x.startswith('__') and x.endswith('__'))]
		self.__dict__.update((k, v) for k, v in kwargs.items() if k in attrs)

		self.store_path = os.path.normpath(os.path.join(self.storage, self.sub))
		self.temp_path = os.path.normpath(os.path.join(cfg.temp_dir, self.sub))
		self.reco_path = os.path.normpath(os.path.join(cfg.arch_path_recompress, self.sub))

	def init_files(self, manager):
		logger.debug(f"Init files from '{self.store_path}'")
		check_dir(self.store_path)
		self.result_files = get_files_info(None)
		self.files = self.filter_files(get_files_info(self.store_path, self.pattern))

	@staticmethod
	def filter_files(files):
		pass

	def get_files_date(self):
		return self.files.date.to_list() if self.files is not None else []
