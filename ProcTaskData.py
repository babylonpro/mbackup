from ProcTask import ProcTask
import os


class ProcTaskData(ProcTask):
	pattern = "*.tar.gz"

	@staticmethod
	def filter_files(files):
		if files.empty:
			return files
		return files[files.date != files.date.max()]

	def init_files(self):
		super().init_files()
		self.temp_path = os.path.join(self.temp_path, "full")
		dates = self.get_files_date()
		taskdata = {'ztype': 'datadiff', 'parent': self, 'storage': self.storage, 'sub': self.sub}
		for date in dates:
			taskdata['fulldate'] = date
			self.manager.add_task(**taskdata)

	def init_files_info(self):
		super().init_files_info()
		self.files_info['arch_name'] \
			= os.path.basename(self.files['path'].get(self.files.first_valid_index(), '')).replace(self.get_ext(), '.7z')
