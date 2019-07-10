from ProcTask import ProcTask


class ProcTaskDataDiff(ProcTask):
	pattern = "*.tar.bz2"

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.ztype = 'diffdata'

	@staticmethod
	def filter_files(files, fulldates=None):
		if files.empty:
			return files
		fulldates = [] if fulldates is None else fulldates
		return files[files.date_diff.isin(fulldates)]
