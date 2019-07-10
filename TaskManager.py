import config as cfg

from ProcTask import ProcTask
from ProcTaskData import ProcTaskData
from ProcTaskDataDiff import ProcTaskDataDiff
from ProcTaskSingle import ProcTaskSingle
from ProcTaskSingleAll import ProcTaskSingleAll

import yaml
from typing import List
import logging

logger = logging.getLogger(f"__main__.{__name__}")


class TaskManager(object):
	def __init__(self):
		self.tasks: List[ProcTask] = []
		self._ztype_map = {
			'data': ProcTaskData,
			'single': ProcTaskSingle,
			'datadiff': ProcTaskDataDiff,
			'singleall': ProcTaskSingleAll
		}
		self.get_tasks()

	def get_tasks(self):
		try:
			with open(cfg.path_mbackup_tasks, 'r') as ymlfile:
				configtasks = yaml.safe_load(ymlfile)
				for storage, item in configtasks.items():
					for sub, ztype in item.items():
						self.add_task(storage=storage, sub=sub, ztype=ztype)
		except Exception as e:
			logger.critical(f"Error while get tasks from '{cfg.path_mbackup_tasks}': {str(e)}")
			raise e

	def add_task(self, **kwargs):
		if 'ztype' in kwargs:
			if kwargs['ztype'] in self._ztype_map:
				task = self._ztype_map[kwargs['ztype']](**kwargs)
				self.tasks.append(task)

	def init_files(self):
		for t in self.tasks:
			t.init_files(self)
