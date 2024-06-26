from abc import ABC
from pathlib import Path


class PathPattern(ABC):
    def __init__(self):
        pass

    def get_paths(self, instrument_tag, dataset):
        raise NotImplementedError()


class PlainPath(PathPattern):
    def __init__(self, base_dir):
        super().__init__()
        self.base_dir = Path(base_dir)

    def get_paths(self, instrument_tag, dataset):
        return [self.base_dir / f"{instrument_tag}_{dataset}.d"]

class GlobPath(PathPattern):
    def __init__(self, base_dir):
        super().__init__()
        self.base_dir = Path(base_dir)

    def get_paths(self, instrument_tag, dataset):
        return [self.base_dir / f"{instrument_tag}*_{dataset}.d"]

class MainzPaths(PathPattern):
    def __init__(self, path="/mnt/ms/old/rawdata/"):
        super().__init__()
        self.path = Path(path)

    def get_paths(self, instrument_tag, dataset):
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
        if instrument_tag == "B":
            glob_patterns.extend(
                [
                    f"/mnt/ms/new/processed/midia_datasets_benchmark/2024_benchmarksets_BremenSK_rawdata/longgradient_highload/{instrument_tag}*_{dataset}.d",
                    f"/mnt/ms/new/processed/midia_datasets_benchmark/2024_benchmarksets_BremenSK_rawdata/shortgradient_highload/{instrument_tag}*_{dataset}.d",
                    f"/mnt/ms/new/processed/midia_datasets_benchmark/2024_benchmarksets_BremenSK_rawdata/Yeast/Yeast15min/{instrument_tag}*_{dataset}.d",
                ]
            )

        print(glob_patterns)
        return glob_patterns
