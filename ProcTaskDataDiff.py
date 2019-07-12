from ProcTask import ProcTask
import config as cfg


class ProcTaskDataDiff(ProcTask):
	pattern = "*.tar.bz2"
	parent = None

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.fulldate = kwargs['fulldate']
		print(self.files)

	def filter_files(self, files):
		if files.empty:
			return files
		return files[files.date_diff == self.fulldate]

	def init_files_info(self):
		super().init_files_info()
		print(self.parent)
		if self.parent is not None:
			print(self.parent.files_info['period'])
			self.files_info['period'] = self.parent.files_info['period']
		self.files_info['arch_name'] \
			= f"{self.files_info['basename']}.{self.fulldate.strftime(cfg.filename_date_formats[0])}_diffs.7z"
