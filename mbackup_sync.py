from logger import set_logger
from mbackup_lib import *
from TaskManager import TaskManager
logger = set_logger('__main__', os.path.basename(__file__))


def subpath_sync(subs):
	logger.info(f"Sync subs #{len(subs)}")
	for task in subs:
		src = os.path.normpath(os.path.join(cfg.mbackup_root_path, task.sub))
		dst = task.store_path
		backup_directory_simple(src, dst)


def main():
	# Проверяем наличие необходимых директорий
	check_dir(cfg.mbackup_root_path)
	tm = TaskManager()
	subpath_sync(tm.tasks)


if __name__ == "__main__":
	main()
