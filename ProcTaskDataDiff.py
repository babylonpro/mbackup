from ProcTask import ProcTask


class ProcTaskDataDiff(ProcTask):
	pattern = "*.tar.bz2"

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.fulldate = kwargs['fulldate']

	def filter_files(self, files):
		if files.empty:
			return files
		return files[files.date_diff == self.fulldate]
