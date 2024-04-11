from abc import ABC
from pathlib import Path
from glob import glob
import subprocess
import shutil


class DataSource(ABC):
    #    def push(self):
    #        raise NotImplemented()
    def fetch(self, instrument, dataset, dst_path):
        raise NotImplemented()

    def push_path(self):
        raise NotImplemented()

    def prepare_dst(self, destination, overwrite):
        destination = Path(destination)
        assert destination.parent.exists()
        if overwrite:
            shutil.rmtree(destination, ignore_errors=True)
        else:
            assert not destination.exists()


class DiskSource(DataSource):
    def __init__(self, path_pattern):
        self.pattern = path_pattern

    def _path_fetch(self, src_path, dst_path):
        command = [
            "cp",
            "--reflink=auto",
            "-r",
            "--no-preserve=all",
            str(src_path),
            str(dst_path),
        ]
        print("Running: " + " ".join(command))
        subprocess.run(command, check=True)

    def fetch(self, instrument_tag, dataset, dst_path, overwrite=False):
        self.prepare_dst(dst_path, overwrite=overwrite)
        src_path = self.pattern.get_paths(instrument_tag, dataset)
        assert len(src_path) == 1
        src_path = src_path[0]
        self._path_fetch(src_path, dst_path)


class SshSource(DataSource):
    def __init__(self, remote_host, path_pattern):
        self.remote = remote_host
        self.pattern = path_pattern

    def fetch(self, instrument_tag, dataset, dst_path, overwrite=False):
        self.prepare_dst(dst_path, overwrite=overwrite)

        for path in self.pattern.get_paths(instrument_tag, dataset):
            remote_path = self.remote + ":" + path
            command = ["scp", "-r", remote_path, dst_path]

            print(f"Attempting fetch of {instrument_tag}{dataset} from {remote_path}")
            ret = subprocess.run(command)
            if ret.returncode == 0:

                print("Data fetch successful!")
                return True
            print(f"Failed, returncode: {ret.returncode}")
        return False


class MainzNetDisk(DiskSource):
    def __init__(self):
        # Composition over inheritance
        self.path = Path("/mnt/ms/old/rawdata/")
        self.disk = None
        if self.path.is_dir():
            self.disk = DiskSource(self.path)

    def fetch(self, instrument_tag, dataset, dst_path):
        if self.disk is None:
            raise FileNotFoundError(f"Path {self.path} does not exist")
        instrument_name = {
            "B": "bruker",
            "F": "falbala",
            "G": "gutamine",
            "O": "obelix",
        }[instrument_tag]

        glob_patterns = [  # general server mounts, on Tenzer Lab VMs
            f"/mnt/ms/old/rawdata/{instrument_name}/{folder}/*/{instrument_tag}*_{dataset}.d"
            for folder in ("ARCHIVIERT", "WIRD_GESICHERT")
        ]
        glob_patterns.append(
            f"/mnt/ms/old/rawdata/{instrument_name}/RAW/{instrument_tag}*_{dataset}.d"
        )
        glob_patterns.append(
            f"/mnt/ms/old/rawdata/{instrument_name}/RAW_ttp/{instrument_tag}*_{dataset}.d"
        )

        paths = set()
        for pattern in glob_patterns:
            paths.update(glob(pattern))

        if len(paths) == 0:
            raise FileNotFoundError(
                f"Can't find any file corresponding to: {instrument_tag}{dataset}"
            )

        if len(paths) > 1:
            raise RuntimeError(
                f"Too many files matching {instrument_tag}{dataset}:\n"
                + "\n".join(paths)
            )

        self.disk._path_fetch(paths.pop(), dst_path)


class Cache(DataSource):
    def __init__(self, back_source, path=None):
        if path is None:
            path = self._default_locations()
        path = Path(path)
        assert path.is_dir()

        self.path = path
        self.back_source = back_source
        self.path_pattern = PlainPath(path)
        self.disk_source = DiskSource(self.path_pattern)

    def _default_locations(self, hostname=None):
        if hostname is None:
            import socket

            hostname = socket.gethostname()
        try:
            return {
                "solace": "/mnt/storage/science/midia_rawdata",
            }
        except KeyError:
            return None

    def _cache_path(self, instrument_tag, dataset):
        paths = self.path_pattern.get_paths(instrument_tag, dataset)
        assert len(paths) == 1
        return paths[0]

    def fetch(self, instrument_tag, dataset, path, overwrite=False):
        path_in_cache = self._cache_path(instrument_tag, dataset)
        finished_tag = path_in_cache.with_suffix(".finished")
        if not finished_tag.exists():
            if not self.back_source.fetch(
                instrument_tag, dataset, path_in_cache, overwrite=True
            ):
                return False
            finished_tag.touch()
        return self.disk_source.fetch(
            instrument_tag, dataset, path, overwrite=overwrite
        )


if __name__ == "__main__":
    m = MainzPaths()
    s = SshSource("tuntiger", MainzPaths())

    # s.fetch("G", 8205, "over", overwrite=True)
    fetcher = Cache(s, "test_cache")
    # fetcher = Cache(s)
    print(fetcher.fetch("G", 8206, "dc"))
