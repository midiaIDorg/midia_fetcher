from midia_fetcher.datasource import DataSource

from pathlib import Path
import boto3


class AwsSource(DataSource):
    def __init__(
        self,
        bucket_name="mainz-shared-bucket",
        region_name=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
    ):
        self.session = boto3.session.Session(region_name=region_name,
                                             aws_access_key_id=aws_access_key_id,
                                             aws_secret_access_key=aws_secret_access_key)
        self.s3 = self.session.resource("s3")
        self.bucket = self.s3.Bucket(bucket_name)

    def fetch(self, instrument, dataset, dst_path, overwrite=False):
        self.prepare_dst(destination=dst_path, overwrite=overwrite)
        dst_path = Path(dst_path)
        paths = set()
        expected_suffix = f"_{dataset}.d"
        for obj in self.bucket.objects.all():
            key = obj.key
            path_n, file_n = key.rsplit("/", 1)
            if file_n == 'analysis.tdf':
                _, dir_n = path_n.rsplit("/", 1)
                if dir_n.startswith(instrument) and dir_n.endswith(expected_suffix):
                    paths.add(path_n)
        if len(paths) > 1:
            raise RuntimeError(f"More than 1 key matching {instrument}*{expected_suffix}/analysis.tdf found. Here's a list:\n{paths}")
        if len(paths) == 0:
            raise FileNotFoundError(f"Can't find file matching {instrument}*{expected_suffix}/analysis.tdf in AWS S3 bucket {self.bucket.name}")

        path = paths.pop()

        for obj in self.bucket.objects.all():
            key = obj.key
            if key.startswith(path):
                filename = key[len(path)+1:]
                target_file = dst_path / filename
                target_file.parent.mkdir(parents=True, exist_ok=True)
                print(key, dst_path, target_file)
                self.bucket.download_file(key, str(target_file))

        return True



if __name__ == "__main__":

    A = AwsSource()
    A.fetch("G", 1234, "gtest", overwrite=True)



