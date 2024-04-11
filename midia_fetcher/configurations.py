import socket

from midia_fetcher.paths import MainzPaths
from midia_fetcher.ssh import SshSource
from midia_fetcher.local import Cache, DiskSource
def get_configuration(name = None):
    if name is None:
        name = socket.gethostname()

    if name == 'solace':
        remote = SshSource("tuntiger", MainzPaths())
        return Cache(remote, "/mnt/storage/science/midia_rawdata")
    elif name == 'tiger':
        remote = DiskSource(MainzPaths())
        return Cache(remote, "/mnt/btrfs/midia_cached_data")
    raise NotImplementedError()