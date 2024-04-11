from midia_fetcher.datasource import DataSource

import subprocess


class SshSource(DataSource):
    def __init__(self, remote_host, path_pattern):
        self.remote = remote_host
        self.pattern = path_pattern

    def fetch(self, instrument, dataset, dst_path, overwrite=False):
        self.prepare_dst(dst_path, overwrite=overwrite)

        for path in self.pattern.get_paths(instrument, dataset):
            remote_path = self.remote + ":" + path
            command = ["scp", "-r", remote_path, dst_path]

            print(f"Attempting fetch of {instrument}{dataset} from {remote_path}")
            ret = subprocess.run(command)
            if ret.returncode == 0:

                print("Data fetch successful!")
                return True
            print(f"Failed, returncode: {ret.returncode}")
        return False
