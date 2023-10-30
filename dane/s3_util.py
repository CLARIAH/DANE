import boto3
import logging
import ntpath
import os
from pathlib import Path
import tarfile
from typing import List, Tuple, Optional


logger = logging.getLogger("DANE")
COMPRESSED_TAR_EXTENSION = ".tar.gz"


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


def is_valid_tar_path(archive_path: str) -> bool:
    logger.info(f"Validating {archive_path}")
    if not os.path.exists(Path(archive_path).parent):
        logger.error(f"Parent dir does not exist: {archive_path}")
        return False
    if archive_path[-7:] != COMPRESSED_TAR_EXTENSION:
        logger.error(
            f"Archive file should have the correct extension: {COMPRESSED_TAR_EXTENSION}"
        )
        return False
    return True


def tar_list_of_files(archive_path: str, file_list: List[str]) -> bool:
    logger.info(f"Tarring {len(file_list)} into {archive_path}")
    if not is_valid_tar_path(archive_path):
        return False
    try:
        with tarfile.open(archive_path, "w:gz") as tar:
            for item in file_list:
                logger.info(os.path.basename(item))
                tar.add(item, arcname=os.path.basename(item))
        logger.info(f"Succesfully created {archive_path}")
        return True
    except tarfile.TarError:
        logger.exception(f"Failed to created archive: {archive_path}")
    except FileNotFoundError:
        logger.exception("File in file list not found")
    except Exception:
        logger.exception("Unhandled error")
    logger.error("Unknown error")
    return False


def validate_s3_uri(s3_uri: str) -> bool:
    if s3_uri[0:5] != "s3://":
        logger.error(f"Invalid protocol in {s3_uri}")
        return False
    if len(s3_uri[5:].split("/")) < 2:
        logger.error(f"No object_name specified {s3_uri}")
        return False
    return True


# e.g. "s3://beng-daan-visxp/jaap-dane-test/dane-test.tar.gz"
def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    logger.info(f"Parsing s3 URI {s3_uri}")
    tmp = s3_uri[5:]
    bucket = tmp[: tmp.find("/")]  # beng-daan-visxp
    object_name = s3_uri[len(bucket) + 6 :]  # jaap-dane-test/dane-test.tar.gz
    return bucket, object_name


def download_s3_uri(s3_uri: str, output_folder: str) -> bool:
    if not validate_s3_uri(s3_uri):
        logger.error("Invalid S3 URI")
        return False
    s3_store = S3Store()
    bucket, object_name = parse_s3_uri(s3_uri)
    return s3_store.download_file(bucket, object_name, output_folder)


class S3Store:

    """
    requires environment:
        - "AWS_ACCESS_KEY_ID=your-key"
        - "AWS_SECRET_ACCESS_KEY=your-secret"
    TODO read from .aws/config, so boto3 can assume an IAM role
    see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

    """

    def __init__(self, s3_endpoint_url: Optional[str] = None, unit_testing=False):
        self.client = boto3.client("s3", endpoint_url=s3_endpoint_url)

    def transfer_to_s3(
        self, bucket: str, path: str, file_list: List[str], tar_archive_path: str = ""
    ) -> bool:
        # first check if the file_list needs to be compressed (into tar)
        if tar_archive_path:
            tar_location = tar_list_of_files(tar_archive_path, file_list)
            if not tar_location:
                logger.error(
                    "Could not archive the file list before transferring to S3"
                )
                return False

            file_list = [tar_archive_path]  # now the file_list just has the tar

        # now go ahead and upload whatever is in the file list
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

    def download_file(self, bucket: str, object_name: str, output_folder: str) -> bool:
        logger.info(f"Downloading {bucket}:{object_name} into {output_folder}")
        if not os.path.exists(output_folder):
            logger.info("Output folder does not exist, creating it...")
            os.makedirs(output_folder)
        output_file = os.path.join(output_folder, os.path.basename(object_name))
        try:
            with open(output_file, "wb") as f:
                self.client.download_fileobj(bucket, object_name, f)
        except Exception:
            logger.exception(f"Failed to download {object_name}")
            return False
        return True
