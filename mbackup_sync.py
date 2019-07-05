from logger import set_logger
from mbackup_lib import *
logger = set_logger('__main__', os.path.basename(__file__))


def subpath_sync(subs):
	logger.info(f"Sync subs #{len(subs)}")
	for subpath, dstroot in subs.items():
		src = os.path.join(cfg.mbackup_root_path, subpath)
		dst = os.path.join(dstroot, subpath)
		backup_directory_simple(src, dst)


def main():
	# Проверяем наличие необходимых директорий
	check_dir(cfg.mbackup_root_path)
	subpath_sync(cfg.sub_path_single)
	subpath_sync(cfg.sub_path_data)


if __name__ == "__main__":
	main()
