#!/usr/bin/env python

import argparse

from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a dataset from one of the preconfigured MIDIA sources."
    )
    parser.add_argument("-o", "--output", help="Target path", type=Path, required=True)
    parser.add_argument(
        "-f",
        "--force",
        help="Overwrite the target path if it already exists",
        action="store_true",
    )
    parser.add_argument(
        "-c", "--config", help="Use a predefined config setting", default=None
    )
    parser.add_argument(
        "-i",
        "--instrument-tag",
        help="Instrument tag, usually a single letter",
        required=True,
    )
    parser.add_argument(
        "-d", "--dataset", help="Dataset number, like 8205", required=True, type=int
    )

    args = parser.parse_args()

    from midia_fetcher.configurations import get_configuration

    fetcher = get_configuration(args.config)
    fetcher.fetch(
        instrument=args.instrument_tag,
        dataset=args.dataset,
        dst_path=args.output,
        overwrite=args.force,
    )


if __name__ == "__main__":
    main()
