import shutil
from abc import ABC
from pathlib import Path

from midia_fetcher.local import DiskSource
from midia_fetcher.paths import PlainPath


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

