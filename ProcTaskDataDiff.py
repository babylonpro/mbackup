from ProcTask import ProcTask


class ProcTaskDataDiff(ProcTask):
	pattern = "*.tar.bz2"

	@staticmethod
	def initFrom(cls):
		kwargs = cls.__dict__
		kwargs['ztype'] = 'diffdata'
		print(kwargs)
		return ProcTask(**kwargs)

	def __init__(self, cls):
		super(self).__init__(**kwargs)
		self.ztype = 'diffdata'

	@staticmethod
	def filterFiles(files, fulldates=None):
		if files.empty:
			return files
		fulldates = [] if fulldates is None else fulldates
		return files[files.date_diff.isin(fulldates)]
