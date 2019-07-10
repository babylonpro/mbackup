from ProcTask import ProcTask


class ProcTaskData(ProcTask):
	pattern = "*.tar.gz"

	@staticmethod
	def filter_files(files):
		if files.empty:
			return files
		return files[files.date != files.date.max()]

	def init_files(self, manager):
		super().init_files(manager)
		dates = self.get_files_date()
		taskdata = self.__dict__
		taskdata['ztype'] = 'diffdata'
		for date in dates:
			taskdata['fulldate'] = date
			manager.add_task(**taskdata)
