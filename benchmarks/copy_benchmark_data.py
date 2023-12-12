import os

from hipscat_cloudtests.copy_cloud_directory import copy_tree_from_cloud

if __name__ == "__main__":
    cloud_dir = "abfs://hipscat/benchmarks/"
    local_dir = os.path.join(os.path.dirname(__file__), "_data")

    storage_options = {
        "account_name": os.environ.get("ABFS_LINCCDATA_ACCOUNT_NAME"),
        "account_key": os.environ.get("ABFS_LINCCDATA_ACCOUNT_KEY"),
    }
    copy_tree_from_cloud(cloud_dir, local_dir, storage_options=storage_options, verbose=True)
