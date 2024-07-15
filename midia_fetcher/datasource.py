import shutil
from abc import ABC
from pathlib import Path


class DataSource(ABC):
    #    def push(self):
    #        raise NotImplemented()
    def fetch(self, instrument, dataset, dst_path, overwrite=False):
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


class Chain(DataSource):
    def __init__(self, sources):
        self.sources = sources

    def fetch(self, instrument, dataset, dst_path, overwrite=False):
        last_exc = None
        for source in self.sources:
            try:
                if source.fetch(
                    instrument=instrument,
                    dataset=dataset,
                    dst_path=dst_path,
                    overwrite=overwrite,
                ):
                    return True
            except Exception as e:
                print(str(e))
                last_exc = e
        raise last_exc
        return False
