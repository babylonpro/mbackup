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
		self.getTasks()

	def getTasks(self):
		# noinspection PyBroadException
		try:
			with open(cfg.path_mbackup_tasks, 'r') as ymlfile:
				configtasks = yaml.safe_load(ymlfile)
				for storage, item in configtasks.items():
					for sub, ztype in item.items():
						self.addTask(ProcTask(storage=storage, sub=sub, ztype=ztype))
		except Exception as e:
			logger.critical(f"Error while get tasks from '{cfg.path_mbackup_tasks}': {e}")
			raise e

	def addTask(self, task: ProcTask):
		if isinstance(task, ProcTask):
			if task.ztype == 'data':
				task.__class__ = ProcTaskData
				taskdata = task.__dict__
				addtask = ProcTaskDataDiff(**taskdata)
				self.tasks.append(addtask)
			if task.ztype == 'single':
				task.__class__ = ProcTaskSingle
			if task.ztype == 'singleall':
				task.__class__ = ProcTaskSingleAll
			self.tasks.append(task)

	def initFiles(self):
		for t in self.tasks:
			t.initFiles()
