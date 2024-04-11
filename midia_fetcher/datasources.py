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


if __name__ == "__main__":
    m = MainzPaths()
    s = SshSource("tuntiger", MainzPaths())

    # s.fetch("G", 8205, "over", overwrite=True)
    fetcher = Cache(s, "test_cache")
    # fetcher = Cache(s)
    print(fetcher.fetch("G", 8206, "dc"))
