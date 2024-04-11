from midia_fetcher.midia_fetcher.datasource import DataSource


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

