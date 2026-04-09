import dagster as dg
import s3fs  # type: ignore


class S3Config(dg.Config):
    endpoint_url: str = dg.EnvVar("AWS_ENDPOINT_URL")
    aws_access_key_id: str = dg.EnvVar("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = dg.EnvVar("AWS_SECRET_ACCESS_KEY")
    aws_region: str = dg.EnvVar("AWS_REGION")
    aws_allow_http: str = "false"


class S3Reader:
    def __init__(self, bucket_name: str, s3_options: S3Config):
        self.bucket_name = bucket_name
        self.s3_options = s3_options
        self.s3 = s3fs.S3FileSystem(
            anon=False,
            key=s3_options.aws_access_key_id,
            secret=s3_options.aws_secret_access_key,
            client_kwargs={
                "region_name": s3_options.aws_region,
                "endpoint_url": s3_options.endpoint_url,
            },
        )

    def read_file_bytes(self, file_path: str, n_bytes: int = -1) -> bytes:
        full_path = f"{self.bucket_name}/{file_path}"
        with self.s3.open(full_path, "rb") as f:  # type: ignore
            return f.read(n_bytes)  # type: ignore

    def read_file_str(self, file_path: str, n_bytes: int | None = None) -> str:
        full_path = f"{self.bucket_name}/{file_path}"
        with self.s3.open(full_path, "r") as f:  # type: ignore
            return f.read(n_bytes)  # type: ignore
