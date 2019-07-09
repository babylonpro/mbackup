from datetime import datetime

arch_path = "E:/!uncompress"
arch_path_compl = "G:/complete"

temp_dir = "H:/Temp"

mbackup_root_path = "\\\\10.0.0.47/backup/moodle"

arch_path_storages = ["E:/!_backupshare", "H:/!_backupshare"]
arch_path_recompress = "G:/!recompress"

sub_path_data = dict(zip(["pgtu/data", "mooped/data", "bbb/records", "leot/gitea_data"],
                         [arch_path_storages[1]] + [arch_path_storages[0]] * 3))
sub_path_single = dict(zip(["pgtu/db", "mooped/db", "leot/svn_dump", "leot/gitea", "leot/youtrack"],
                           [arch_path_storages[0]] * 5))

filename_date_formats = ["%Y-%m-%d.%H-%M-%S", "%Y-%m-%d-%H-%M-%S", "%f"]
filename_date_pattern = r"\d{4}-\d{2}-\d{2}[\.|-]\d{2}-\d{2}-\d{2}|\d{10}"

proc_task_poll_size = 2
extracting_pool_size = 5

path_mbackup_tasks = "mbackup_tasks.yaml"
path_checked_archs = "mbackup_proc_checked_archs.csv"
path_backup_stats = f"backup_stats{datetime.strftime(datetime.today(), filename_date_formats[0])}.csv"

min_days = 60
max_days = 120

path_7z = r"C:\Progra~1\7-Zip\7z.exe"
