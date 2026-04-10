# bp_utils.py here

import os
from dagster import resource, InitResourceContext
from ds3 import ds3


class SpectraLogicClient:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.bucket = bucket
        self.client = ds3.createClientFromEnv()
        # Or configure manually:
        # self.client = ds3.Client(endpoint, ds3.Credentials(access_key, secret_key))

    def put_bulk(self, file_list: list[dict]) -> str:
        """
        Submit a batch of files to the tape library.
        file_list: [{"name": obj_name, "path": local_path, "size": int}, ...]
        Returns a job ID from the SpectraLogic system.
        """
        object_list = ds3.FileObjectList(
            [ds3.FileObject(f["name"], f["size"]) for f in file_list]
        )
        bulk_result = self.client.put_bulk_job_spectra_s3(
            ds3.PutBulkJobSpectraS3Request(self.bucket, object_list)
        )
        job_id = bulk_result.result["JobId"]

        # Transfer each file chunk as allocated
        for chunk in bulk_result.result["ObjectsList"]:
            for obj in chunk:
                matching = [f for f in file_list if f["name"] == obj["Name"]][0]
                self.client.put_object(
                    ds3.PutObjectRequest(
                        self.bucket,
                        obj["Name"],
                        length=obj["Length"],
                        offset=obj["Offset"],
                        stream=open(matching["path"], "rb"),
                    )
                )
        return job_id

    def verify_object(self, object_name: str) -> dict:
        """
        Retrieve the stored object metadata for verification.
        Returns dict with 'size' and 'checksum' keys.
        """
        head_response = self.client.head_object(
            ds3.HeadObjectRequest(self.bucket, object_name)
        )
        return {
            "size": head_response.result.get("ContentLength"),
            "checksum": head_response.result.get("ETag", "").strip('"'),
        }


@resource
def spectralogic_client(context: InitResourceContext) -> SpectraLogicClient:
    return SpectraLogicClient(
        endpoint=os.environ["SPECTRALOGIC_ENDPOINT"],
        access_key=os.environ["SPECTRALOGIC_ACCESS_KEY"],
        secret_key=os.environ["SPECTRALOGIC_SECRET_KEY"],
        bucket=os.environ["SPECTRALOGIC_BUCKET"],
    )
