"""
Consolidate all BP activities
to one Class module

2026
"""

import os
import json
from typing import Optional, Union, List, Dict, Any
from dagster import resource, InitResourceContext
from ds3 import ds3, ds3Helpers

CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
JSON_END = os.environ["JSON_END_POINT"]


class SpectraLogicClient:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.bucket = bucket
        self.client = ds3.createClientFromEnv()
        # Or configure manually:
        # self.client = ds3.Client(endpoint, ds3.Credentials(access_key, secret_key))

    """
    def put_bulk(self, file_list: list[dict]) -> str:
        '''
        Submit a batch of files to the tape library.
        file_list: [{"name": obj_name, "path": local_path, "size": int}, ...]
        Returns a job ID from the SpectraLogic system.
        '''
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
        '''
        Retrieve the stored object metadata for verification.
        Returns dict with 'size' and 'checksum' keys.
        '''
        head_response = self.client.head_object(
            ds3.HeadObjectRequest(self.bucket, object_name)
        )
        return {
            "size": head_response.result.get("ContentLength"),
            "checksum": head_response.result.get("ETag", "").strip('"'),
        }
`   """

    def check_no_bp_status(self, fname: str, bucket_list: list[str]) -> bool:
        """
        Look up filename in BP to avoid
        multiple ingests of files
        """
        exist_across_buckets: list[str] = []
        for bucket in bucket_list:
            try:
                query: ds3.HeadObjectRequest = ds3.HeadObjectRequest(bucket, fname)
                result: ds3.HeadObjectResponse = CLIENT.head_object(query)

                # Only return false if DOESNTEXIST is missing, eg file found
                if "DOESNTEXIST" in str(result.result):
                    print(f"File {fname} NOT found in Black Pearl bucket {bucket}")
                    exist_across_buckets.append("DOESNTEXIST")
                elif str(result.result) == "EXISTS":
                    print(f"File {fname} found in Black Pearl bucket {bucket}")
                    exist_across_buckets.append("PRESENT")
            except Exception as err:
                print(err)

        print(exist_across_buckets)
        if exist_across_buckets == []:
            return False
        if "PRESENT" in str(exist_across_buckets):
            return False
        if "DOESNTEXIST" in str(exist_across_buckets):
            return True
        return False

    def get_job_status(self, job_id: str) -> tuple[str, str]:
        """
        Fetch job status for specific ID
        """
        cached = status = ""

        job_status: ds3.GetJobSpectraS3Request = CLIENT.get_job_spectra_s3(
            ds3.GetJobSpectraS3Request(job_id.strip())
        )

        if job_status.result["CachedSizeInBytes"]:
            cached = job_status.result["CachedSizeInBytes"]
        if job_status.result["Status"]:
            status = job_status.result["Status"]
        print(f"Status for JOB ID: {job_id}")
        print(f"{status}, {cached}")

        return status, cached

    def get_bp_md5(self, fname: str, bucket: str) -> Optional[str]:
        """
        Fetch BP checksum to compare
        to new local MD5
        """
        md5: str = ""
        query: ds3.HeadObjectRequest = ds3.HeadObjectRequest(bucket, fname)
        result: ds3.HeadObjectResponse = CLIENT.head_object(query)
        try:
            md5: str = result.response.msg["ETag"]
        except Exception as err:
            print(err)
            return None
        if md5:
            return md5.replace('"', "")

    def get_bp_length(self, fname: str, bucket: str) -> Optional[str]:
        """
        Fetch BP checksum to compare
        to new local MD5
        """
        size: str = ""
        query: ds3.HeadObjectRequest = ds3.HeadObjectRequest(bucket, fname)
        result: ds3.HeadObjectResponse = CLIENT.head_object(query)
        try:
            size = result.response.msg["Content-Length"]
        except Exception as err:
            print(err)
            return None
        if size:
            return size.replace('"', "")

    def get_confirmation_length_md5(
        self,
        fname: str,
        bucket: str,
        bucket_list: list[str]
    ) -> Optional[tuple[Optional[Union[bool, str]], Optional[str], Optional[str]]]:
        """
        Alternative retrieval for get_object_list
        avoiding full_details requests
        """
        flist: list[str] = [fname]
        try:
            object_flist: list[ds3.Ds3GetObject] = list(
                [ds3.Ds3GetObject(name=fname) for fname in flist]
            )
            res = ds3.GetPhysicalPlacementForObjectsSpectraS3Request(bucket, object_flist)
            result = CLIENT.get_physical_placement_for_objects_spectra_s3(res)
            data = result.result
        except Exception as err:
            print(err)
            data = None

        if data is None:
            for buck in bucket_list:
                try:
                    object_flist = list([ds3.Ds3GetObject(name=fname) for fname in flist])
                    res = ds3.GetPhysicalPlacementForObjectsSpectraS3Request(
                        buck, object_flist
                    )
                    result = CLIENT.get_physical_placement_for_objects_spectra_s3(res)
                    print(result.result)
                    if len(result.result["TapeList"]) > 0:
                        data = result.result
                        bucket = buck
                        break
                except Exception as err:
                    data = None
                    print(err)

        if not data["TapeList"]:
            return "No tape list", None, None
        if result.result["TapeList"][0]["AssignedToStorageDomain"] == "true":
            confirmed = True
        elif result.result["TapeList"][0]["AssignedToStorageDomain"] == "false":
            confirmed = False
        else:
            return None, None, None

        md5 = get_bp_md5(fname, bucket)
        length = get_bp_length(fname, bucket)
        return confirmed, md5, length

    def get_object_list(
        self,
        fname: str,
    ) -> Optional[tuple[Union[bool, str], Optional[str], Optional[str]]]:
        """
        Get all details to check file persisted
        """

        request = ds3.GetObjectsWithFullDetailsSpectraS3Request(
            name=f"{fname}", include_physical_placement=True
        )
        try:
            result = CLIENT.get_objects_with_full_details_spectra_s3(request)
            data = result.result
        except Exception as err:
            print(err)
            return None

        if not data["ObjectList"]:
            return "No object list", None, None
        if "'TapeList': [{'AssignedToStorageDomain': 'true'" in str(data):
            confirmed = True
        elif "'TapeList': [{'AssignedToStorageDomain': 'false'" in str(data):
            confirmed = False
        try:
            md5 = data["ObjectList"][0]["ETag"]
        except (TypeError, IndexError):
            md5 = None
        try:
            length = data["ObjectList"][0]["Blobs"]["ObjectList"][0]["Length"]
        except (TypeError, IndexError):
            length = None

        return confirmed, md5, length

    def get_object_list_items(self, fname: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get all details to check file persisted
        """

        request = ds3.GetObjectsWithFullDetailsSpectraS3Request(
            name=f"{fname}", include_physical_placement=True
        )
        try:
            result = CLIENT.get_objects_with_full_details_spectra_s3(request)
            data = result.result
        except Exception as err:
            print(err)
            return None

        try:
            obj_list = data.get("ObjectList")
        except KeyError as err:
            print(err)
            obj_list = []

        return obj_list

    def put_directory(self, directory_pth: str, bucket: str) -> Optional[list[str]]:
        """
        Add the directory to black pearl using helper (no MD5)
        Retrieve job number and launch json notification
        """
        try:
            put_job_ids: list[str] = HELPER.put_all_objects_in_directory(
                source_dir=directory_pth,
                bucket=bucket,
                objects_per_bp_job=5000,
                max_threads=3,
            )
        except Exception as err:
            print("Exception: %s", err)
            return None
        print(f"PUT COMPLETE - JOB ID retrieved: {put_job_ids}")
        job_list = []
        for job_id in put_job_ids:
            job_list.append(job_id)
        return job_list

    def put_notification(self, job_id: str) -> str:
        """
        Ensure job notification is sent to Isilon/ BP NAS
        """
        job_completed_registration = (
            CLIENT.put_job_completed_notification_registration_spectra_s3(
                ds3.PutJobCompletedNotificationRegistrationSpectraS3Request(
                    notification_end_point=JSON_END, format="JSON", job_id=job_id
                )
            )
        )

        return job_completed_registration.result["NotificationEndPoint"]

    def download_bp_object(self, fname: str, outpath: str, bucket: str) -> str:
        """
        Download the BP object from SpectraLogic
        tape library and save to outpath
        """
        if bucket == "":
            bucket = "imagen"

        file_path: str = os.path.join(outpath, fname)
        get_objects: list[ds3Helpers.HelperGetObject] = [
            ds3Helpers.HelperGetObject(fname, file_path)
        ]
        try:
            get_job_id: str = HELPER.get_objects(get_objects, bucket)
            print(f"BP get job ID: {get_job_id}")
        except Exception as err:
            raise Exception(f"Unable to retrieve file {fname} from Black Pearl: {err}")

        return get_job_id

    def download_blobbed_object(self, fname: str, outpath: str, bucket: str) -> str:
        """
        Download the BP object from SpectraLogic
        tape library using single thread
        """
        if bucket == "":
            bucket = "imagen"

        file_path: str = os.path.join(outpath, fname)
        get_objects: list[ds3Helpers.HelperGetObject] = [
            ds3Helpers.HelperGetObject(fname, file_path)
        ]
        try:
            get_job_id: str = HELPER.get_objects(get_objects, bucket, max_threads=1)
            print(f"BP get job ID: {get_job_id}")
        except Exception as err:
            raise Exception(f"Unable to retrieve file {fname} from Black Pearl: {err}")

        return get_job_id

    def put_single_file(self, fpath: str, ref_num, bucket_name, check=False) -> Optional[str]:
        """
        Add the file to black pearl using helper
        Fine for < or > 1TB
        """
        file_size: int = os.path.getsize(fpath)
        put_obj: ds3Helpers.HelperPutObject = [
            ds3Helpers.HelperPutObject(object_name=ref_num, file_path=fpath, size=file_size)
        ]
        try:
            put_job_id: str = HELPER.put_objects(
                put_objects=put_obj,
                bucket=bucket_name,
                max_threads=1,
                calculate_checksum=bool(check),
            )
            print(f"PUT COMPLETE - JOB ID retrieved: {put_job_id}")
            return put_job_id
        except Exception as err:
            print("Exception: %s", err)
            return None


@resource
def spectralogic_client(context: InitResourceContext) -> SpectraLogicClient:
    return SpectraLogicClient(
        endpoint=os.environ["SPECTRALOGIC_ENDPOINT"],
        access_key=os.environ["SPECTRALOGIC_ACCESS_KEY"],
        secret_key=os.environ["SPECTRALOGIC_SECRET_KEY"],
        bucket=os.environ["SPECTRALOGIC_BUCKET"],
    )
