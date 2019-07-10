from ProcTask import ProcTask
from datetime import datetime


class ProcTaskSingle(ProcTask):

	@staticmethod
	def filter_files(files):
		if files.empty:
			return files
		return files[(files.date.dt.month < datetime.today().month) | (files.date.dt.year < datetime.today().year)]
