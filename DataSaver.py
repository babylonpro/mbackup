import pandas as pd
import os


class DataSaver(object):
	def __init__(self, file, cols=None, parse_dates=None):
		self._file = os.path.abspath(file)
		if os.path.isfile(self._file):
			self.data = pd.read_csv(self._file, sep=";", infer_datetime_format=True, parse_dates=parse_dates)
		else:
			cols = [] if cols is None else cols
			self.data = pd.DataFrame(columns=cols)

	def add(self, row):
		self.data = self.data.append(pd.Series(dict(zip(self.data.columns, row))), ignore_index=True)

	def save(self):
		self.data.to_csv(self._file, sep=";", index=False)
