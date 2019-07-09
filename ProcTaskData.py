from ProcTask import ProcTask


class ProcTaskData(ProcTask):
	pattern = "*.tar.gz"

	@staticmethod
	def filterFiles(files):
		if files.empty:
			return files
		return files[files.date != files.date.max()]
