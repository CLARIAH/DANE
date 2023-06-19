import os
import ntpath
import logging
from typing import List

import boto3


logger = logging.getLogger("DANE")


# the file name without extension is used as an asset ID by the ASR container to save the results
def generate_asset_id_from_input_file(
    input_file: str, with_extension: bool = False
) -> str:
    logger.info(f"generating asset ID for {input_file}")
    file_name = ntpath.basename(input_file)  # grab the file_name from the path
    if with_extension:
        return file_name

    # otherwise cut off the extension
    asset_id, extension = os.path.splitext(file_name)
    return asset_id


class S3Store:

    """
    requires environment:
        - "AWS_ACCESS_KEY_ID=your-key"
        - "AWS_SECRET_ACCESS_KEY=your-secret"
    """

    def __init__(self, s3_endpoint_url: str, unit_testing=False):
        self.client = boto3.client("s3", endpoint_url=s3_endpoint_url)

    def transfer_to_s3(self, bucket: str, path: str, file_list: List[str]) -> bool:
        for f in file_list:
            try:
                self.client.upload_file(
                    Filename=f,
                    Bucket=bucket,
                    Key=os.path.join(
                        path,
                        generate_asset_id_from_input_file(  # file name with extension
                            f, True
                        ),
                    ),
                )
            except Exception:  # TODO figure out which Exception to catch specifically
                logger.exception(f"Failed to upload {f}")
                return False
        return True

    def download_file(self, bucket, file_name: str) -> bool:
        try:
            self.client.download_file(Bucket=bucket, Key=file_name, Filename=file_name)
        except Exception:
            logger.exception(f"Failed to download {file_name}")
            return False
        return True
