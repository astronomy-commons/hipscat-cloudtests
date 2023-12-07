"""Testing utility class to create a temporary directory that's local
to some unit test execution."""

import os
import time

import shortuuid
from hipscat.io.file_io import file_io


class TempCloudDirectory:
    """Simple context manager that creates a unique temporary directory
    path for a single testing context.

    On exit, we will recursively remove the created directory."""

    def __init__(self, prefix_path, method_name="", storage_options: dict = None):
        """Create a new context manager.

        This will NOT create the new temp path - that happens when we enter the context.

        Args:
            prefix_path (str): base path to the cloud resource
            method_name (str): optional token to indicate the method under test
            storage_options (dict): dictionary that contains abstract filesystem credentials
        """
        self.prefix_path = prefix_path
        self.method_name = method_name
        self.storage_options = storage_options
        self.temp_path = ""

    def __enter__(self):
        """Create a new temporary path

        Returns:
            string path that's been created. it will take the form of
            <prefix_path>/<method_name><some random string>
        """
        my_uuid = shortuuid.uuid()
        self.temp_path = os.path.join(self.prefix_path, f"{self.method_name}{my_uuid}")
        return self.temp_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Recursively delete the created resources.

        This will try to delete 3 times, with exponential backoff.
        We give up after the third attempt."""
        sleep_time = 2
        if self.temp_path:
            for attempt_number in range(3):
                ## Try
                try:
                    file_io.remove_directory(self.temp_path, storage_options=self.storage_options)
                    return
                except RuntimeError:
                    if attempt_number == 2:
                        print(f"Failed to remove directory {self.temp_path}. Giving up.")
                        return
                print(f"Failed to remove directory {self.temp_path}. Trying again.")
                time.sleep(sleep_time)
                sleep_time *= 2
