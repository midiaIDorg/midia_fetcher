import subprocess
from pathlib import Path

from midia_fetcher.datasource import DataSource
from midia_fetcher.paths import PlainPath


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
        src_paths = self.pattern.get_paths(instrument_tag, dataset)
        globbed_paths = set()
        for path in src_paths:
            globbed_paths.update(path.glob())

        if len(globbed_paths) > 1:
            raise RuntimeError(f"More than one directory matching storage pattern present. Those are: \n{globbed_paths}")
        if len(globbed_paths) == 0:
            raise FileNotFoundError(f"No dataset found matching: {src_paths}")
        src_path = globbed_paths.pop()
        self._path_fetch(src_path, dst_path)
        return True

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
            print(f"Finished-tag {finished_tag} not present in cache, fetching from remote into cache")
            if not self.back_source.fetch(
                instrument_tag, dataset, path_in_cache, overwrite=True
            ):
                print("Cache: Couldn't fetch remote dataset, giving up")
                return False
            finished_tag.touch()
        print("Copying from cache")
        return self.disk_source.fetch(
            instrument_tag, dataset, path, overwrite=overwrite
        )

