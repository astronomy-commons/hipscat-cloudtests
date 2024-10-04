"""Testing utility class to create a temporary directory that's local
to some unit test execution."""

import time

import shortuuid
from hats.io.file_io import file_io


class TempCloudDirectory:
    """Simple context manager that creates a unique temporary directory
    path for a single testing context.

    On exit, we will recursively remove the created directory."""

    def __init__(self, prefix_path, method_name="", real_directories=True):
        """Create a new context manager.

        This will NOT create the new temp path - that happens when we enter the context.

        Args:
            prefix_path (UPath): base path to the cloud resource
            method_name (str): optional token to indicate the method under test
            real_directories (bool): are directories in this file system real, and
                should be deleted along with other temp content?
        """
        self.prefix_path = prefix_path
        self.method_name = method_name
        self.temp_path = ""
        self.real_directories = real_directories

    def __enter__(self):
        """Create a new temporary path

        Returns:
            UPath object that's been created. it will take the form of
            <prefix_path>/<method_name><some random string>
        """
        return self.open()

    def open(self):
        """Create a new temporary path

        Returns:
            UPath object that's been created. it will take the form of
            <prefix_path>/<method_name><some random string>
        """
        my_uuid = shortuuid.uuid()
        self.temp_path = self.prefix_path / f"{self.method_name}-{my_uuid}"
        return self.temp_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Recursively delete the created resources.

        This will try to delete 3 times, with exponential backoff.
        We give up after the third attempt."""
        self.close()

    def close(self, num_retries=4):
        """Recursively delete the created resources.

        This will try to delete `num_retries` times, with exponential backoff.
        We give up after the last attempt."""
        sleep_time = 2
        if self.temp_path:
            for attempt_number in range(1, num_retries + 1):
                ## Try
                try:
                    if self.real_directories:
                        file_io.remove_directory(self.temp_path)
                    else:
                        _try_remove_contents(self.temp_path)
                    return
                except RuntimeError:
                    if attempt_number == num_retries:
                        print(f"Failed to remove directory {self.temp_path}. Giving up.")
                        return
                print(f"Failed to remove directory {self.temp_path}. Trying again.")
                time.sleep(sleep_time)
                sleep_time *= 2


def _try_remove_contents(directory):
    for item in directory.iterdir():
        if item.is_dir():
            _try_remove_contents(item)
        else:
            item.unlink()
