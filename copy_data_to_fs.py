import os

from hipscat_cloudtests.copy_cloud_directory import copy_tree_fs_to_fs

if __name__ == "__main__":
    source_pw = f"{os.getcwd()}/../tests/data"
    target_pw = "abfs://hipscat/pytests/lsdb"

    target_so = {
        "account_name": os.environ.get("ABFS_LINCCDATA_ACCOUNT_NAME"),
        "account_key": os.environ.get("ABFS_LINCCDATA_ACCOUNT_KEY"),
    }
    copy_tree_fs_to_fs(source_pw, target_pw, {}, target_so, verbose=True)
