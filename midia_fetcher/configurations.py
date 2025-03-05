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
    if os.environ.get("MIDIA_FETCHER_CONFIG") is not None:
        print(
            f'Using `MIDIA_FETCHER_CONFIG = {os.environ.get("MIDIA_FETCHER_CONFIG")}`'
        )

        cfg_path = Path(os.environ.get("MIDIA_FETCHER_CONFIG")).expanduser()
        if not cfg_path.exists():
            raise FileNotFoundError(
                f"Configuration file {cfg_path} does not exist. Either create it or unset the MIDIA_FETCHER_CONFIG environment variable."
            )
        with open(cfg_path, "r") as f:
            cfg = f.read()
            local_namespace = {}
            global_namespace = globals()
            exec(cfg, global_namespace, local_namespace)

            if global_namespace.get("fetcher", ""):
                print("`fetcher` in global env")
                return global_namespace.get("fetcher", "")
            if local_namespace.get("fetcher", ""):
                print("`fetcher` in local env")
                return local_namespace.get("fetcher", "")
            raise NameError(
                'The configuration file must define a variable named "fetcher".'
            )

    print("Using default config.")
    if name is None:
        name = socket.gethostname()

    match socket.gethostname():
        case "solace":
            ssh = SshSource("tuntiger", MainzPaths())
            aws = default_aws_source()
            remote = Chain([ssh, aws])
            fetcher = Cache(remote, "/mnt/storage/science/midia_rawdata")
        case "spot" | "DESKTOP-2T68Q8K":
            remote = SshSource("tuntiger", MainzPaths())
            fetcher = Cache(remote, Path("~/midia_rawdata").expanduser())
        case "nighthaven":
            remote = SshSource("tuntiger", MainzPaths())
            fetcher = Cache(remote, "/mnt/storage/midia/rawdata")
        case "wloczykij":
            ssh = SshSource("tuntiger", MainzPaths())
            aws = default_aws_source()
            remote = Chain([ssh, aws])
            fetcher = Cache(remote, "/home/midia_rawdata")
        case "pingu":
            remote = Chain(
                [
                    SshSource("midia", MainzPaths()),
                    SshSource("midia", NewMainzPaths()),
                    default_aws_source(),
                ]
            )
            fetcher = Cache(remote, "/home/matteo/msdata")
        case "ub24":
            local = Chain(
                [
                    DiskSource(Path("~/midia_cached_data").expanduser()),
                    DiskSource(MainzPaths()),
                    DiskSource(NewMainzPaths()),
                ]
            )
            aws = Cache(default_aws_source(), Path("~/aws_cache").expanduser())
            fetcher = Chain([local, aws])
        case "midia_docker":
            set_aws_entry("AWS_CONFIG_FILE", "configs/aws/config")
            set_aws_entry("AWS_CREDENTIALS_FILE", "configs/aws/credentials")
            fetcher = AwsSource(prefixes=["data/reference_datasets"])
        case other:  # like default
            raise NotImplementedError(
                "In general, how are we supposed to know where to fetch data?\n Currently we do it by hostname (check `configs/fetcher/default.py`).\nPut your data in folder `spectra` or specify rules for getting the data in `configs/fetcher/default.py`."
            )

    return fetcher
