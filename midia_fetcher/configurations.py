import os
import socket

from pathlib import Path

from midia_fetcher.aws import AwsSource
from midia_fetcher.datasource import Chain
from midia_fetcher.local import Cache
from midia_fetcher.local import DiskSource
from midia_fetcher.paths import MainzPaths
from midia_fetcher.ssh import SshSource


def default_aws_source():
    # return AwsSource(prefixes=["data/reference_datasets", "data/reference_data"])
    return AwsSource(
        prefixes=[
            "data/reference_datasets",
        ]
    )


def get_configuration(name=None):
    print(os.environ.get("MIDIA_FETCHER_CONFIG"))
    if os.environ.get("MIDIA_FETCHER_CONFIG") is not None:
        cfg_path = Path(os.environ.get("MIDIA_FETCHER_CONFIG")).expanduser()
        if not cfg_path.exists():
            raise FileNotFoundError(
                f"Configuration file {cfg_path} does not exist. Either create it or unset the MIDIA_FETCHER_CONFIG environment variable."
            )
        with open(cfg_path, "r") as f:
            cfg = "global fetcher\n" + f.read()
            exec(cfg)
            try:
                return fetcher
            except NameError:
                raise NameError(
                    'The configuration file must define a variable named "fetcher".'
                )

    if name is None:
        name = socket.gethostname()

    if name == "solace":
        ssh = SshSource("tuntiger", MainzPaths())
        aws = default_aws_source()
        remote = Chain([ssh, aws])
        return Cache(remote, "/mnt/storage/science/midia_rawdata")
    elif name in ["spot", "DESKTOP-2T68Q8K"]:
        remote = SshSource("tuntiger", MainzPaths())
        return Cache(remote, Path("~/midia_rawdata").expanduser())
    elif name == "nighthaven":
        remote = SshSource("tuntiger", MainzPaths())
        return Cache(remote, "/mnt/storage/midia/rawdata")
    elif name == "wloczykij":
        ssh = SshSource("tuntiger", MainzPaths())
        aws = default_aws_source()
        remote = Chain([ssh, aws])
        return Cache(remote, "/home/midia_rawdata")
    elif name == "tiger":
        remote = DiskSource(MainzPaths())
        return Cache(remote, "/mnt/btrfs/midia_cached_data")
    elif name == "midiaor":
        remote = DiskSource(MainzPaths())
        return Cache(remote, "/home/_common_/midia_cached_data")
    elif name in ("oldjugular", "jagular"):
        remote = DiskSource(MainzPaths())
        return Cache(remote, "/shared/midia_cached_data")
    elif name == "pingu":
        ssh = SshSource("tiger", MainzPaths())
        aws = default_aws_source()
        remote = Chain([ssh, aws])
        return Cache(remote, "/home/matteo/msdata")
    else:
        aws = Cache(default_aws_source(), Path("~/aws_cache").expanduser())
        local = DiskSource(Path("~/data").expanduser())
        return Chain([local, aws])
    """
    else:
        aws = default_aws_source()
        cache_folder = Path.home() / "msdata"
        cache_folder.mkdir(parents=False, exist_ok=True)
        return Cache(aws, str(cache_folder))
    raise NotImplementedError()
    """
