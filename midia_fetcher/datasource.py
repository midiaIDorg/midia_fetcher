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
        last_exc.add_note(
            "In general, how are we supposed to know where to fetch data?\nPut your data in folder `spectra` or specify rules for getting the data in `configs/fetcher/default.py`."
        )
        raise last_exc
        return False
