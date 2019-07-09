from mbackup_lib import *


class ProcTask(object):
	storage = ''
	sub = ''
	ztype = ''
	pattern = '*'

	files = None
	result_files = None

	def __init__(self, **kwargs):
		self.__dict__.update((k, v) for k, v in kwargs.items() if k in self.__class__.__dict__.keys())

		self.src_path = os.path.join(self.storage, self.sub)
		self.temp_path = os.path.join(cfg.temp_dir, self.sub)
		self.dst_path = os.path.join(cfg.arch_path_recompress, self.sub)

	def initFiles(self):
		check_dir(self.src_path)
		self.result_files = get_files_info(None)
		self.files = self.filterFiles(get_files_info(self.src_path, self.pattern))
		# logger.info(f"Proc sub '{subpath}'")

	@staticmethod
	def filterFiles(files):
		pass
