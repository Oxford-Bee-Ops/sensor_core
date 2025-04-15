from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from azure.storage.blob import BlobClient, BlobLeaseClient, ContainerClient, StandardBlobTier

from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming as fn

logger = root_cfg.setup_logger(name="sensor_core")


class CloudConnector:
    def __init__(self) -> None:
        if root_cfg.my_device is None:
            raise ValueError("System configuration not set; cannot connect to cloud")

        if root_cfg.keys is None or root_cfg.keys.cloud_storage_key == root_cfg.FAILED_TO_LOAD:
            raise ValueError("Cloud storage credentials not set; cannot connect to cloud")

        self._connection_string = root_cfg.keys.cloud_storage_key

    def upload_to_cloud(
        self, dst_container: str | ContainerClient, src_files: list[Path], delete_src: Optional[bool] = True
    ) -> None:
        """Upload a list of local files to a CloudDatastore
        These will be regular block_blobs and not append_blobs; see append_to_cloud for append_blobs.

        Parameters
        ----------
        dst_datastore: destination CloudDatastore
        src_files: list of files to upload to the CloudDatastore
        delete_src: delete the local src_files instances after successful upload; defaults to True

        If the upload fails part way through, those files that were successfully uploaded will have
        been deleted (if delete_src=True), while any remaining files in src_files will not have been
        deleted.
        """
        upload_container = self._validate_container(dst_container)

        for file in src_files:
            if file.exists():
                blob_client = upload_container.get_blob_client(file.name)
                with open(file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True, connection_timeout=600)
                if delete_src:
                    file.unlink()

    def upload_to_container(
        self,
        dst_container: str | ContainerClient,
        src_files: list[Path],
        delete_src: Optional[bool] = True,
        standard_blob_tier: Enum=StandardBlobTier.Hot,
    ) -> None:
        """Upload a list of local files to an Azure container
        These will be regular block_blobs and not append_blobs; see append_to_cloud for append_blobs.

        Parameters
        ----------
        dst_container: destination ContainerClient
        src_files: list of files to upload to the container
        delete_src: delete the local src_files instances after successful upload; defaults to True

        If the upload fails part way through, those files that were successfully uploaded will have
        been deleted (if delete_src=True), while any remaining files in src_files will not have been
        deleted.
        """
        upload_container = self._validate_container(dst_container)

        if not isinstance(src_files, list):
            src_files = [src_files]

        for file in src_files:
            if file.exists():
                blob_client = upload_container.get_blob_client(file.name)
                with open(file, "rb") as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        connection_timeout=600,
                        standard_blob_tier=standard_blob_tier,
                    )
                if delete_src:
                    logger.debug(f"Deleting uploaded file: {file}")
                    file.unlink()
            else:
                logger.error(f"Upload failed because file {file} does not exist")

    def download_from_container(
        self, src_container: str | ContainerClient, src_file: str, dst_file: Path
    ) -> None:
        """Downloads the src_datafile to a local dst_file Path."""

        if dst_file is None or not isinstance(dst_file, Path):
            return

        download_container = self._validate_container(src_container)

        blob_client = download_container.get_blob_client(src_file)
        with open(dst_file, "wb") as my_file:
            download_stream = blob_client.download_blob()
            my_file.write(download_stream.readall())

    def download_container(
        self,
        src_container: str | ContainerClient,
        dst_dir: Path,
        folder_prefix_len: Optional[int] = None,
        files: Optional[list[str]] = None,
        overwrite: Optional[bool] = True,
    ) -> None:
        """Download all the files in the src_datastore to the dst_dir

        Parameters
        ----------
        src_datastore: source CloudDatastore
        dst_dir: destination directory to download the files to
        folder_prefix_len: Optional; first n characters of the file name to use as a subfolder
        files: Optional; list of files to download from src_datastore; if None, all files in the container
            will be downloaded; useful for chunking downloads
        """

        download_container = self._validate_container(src_container)
        original_dst_dir = dst_dir

        if files is None:
            for blob in download_container.list_blobs():
                blob_client = download_container.get_blob_client(blob.name)
                if folder_prefix_len is not None:
                    dst_dir = original_dst_dir.joinpath(blob.name[:folder_prefix_len])
                    if not dst_dir.exists():
                        dst_dir.mkdir(parents=True, exist_ok=True)
                with open(dst_dir.joinpath(blob.name), "wb") as my_file:
                    download_stream = blob_client.download_blob()
                    my_file.write(download_stream.readall())
        else:
            files_downloaded = 0
            # Create a pool of threads to download the files
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = []
                for blob_name in files:
                    blob_client = download_container.get_blob_client(blob_name)
                    if folder_prefix_len is not None:
                        dst_dir = original_dst_dir.joinpath(blob_name[:folder_prefix_len])
                    dst_file = dst_dir.joinpath(blob_name)
                    if not overwrite and dst_file.exists():
                        logger.debug(f"File {dst_file} already exists; skipping download")
                        continue
                    futures.append(executor.submit(self._download_file, blob_client, dst_file))
                    if len(futures) > 10000:
                        logger.info("Working on batch of 10000 files")
                        for future in as_completed(futures, timeout=600):
                            future.result()
                            files_downloaded += 1
                        futures = []
                logger.info(f"Downloading total of {len(futures)} files")

                for future in as_completed(futures, timeout=600):
                    future.result()
                    files_downloaded += 1
                logger.info(f"Completed downloaded of {files_downloaded} files")

    def _download_file(self, src: BlobClient, dst_file: Path) -> str:
        """Download a single file"""

        if not dst_file.parent.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)
        with open(dst_file, "wb") as my_file:
            download_stream = src.download_blob()
            my_file.write(download_stream.readall())
        return dst_file.name

    def move_between_containers(
        self,
        src_container: str | ContainerClient,
        dst_container: str | ContainerClient,
        blob_names: list[str],
        delete_src: bool = False,
        standard_blob_tier: Enum =StandardBlobTier.Cool,
    ) -> None:
        """Move blobs between containers

        Parameters
        ----------
        src_container: source container
        dst_container: destination container
        blob_names: list of blob names to move
        delete_src: delete the source blobs after successful upload; defaults to False
        """
        src_container = self._validate_container(src_container)
        dst_container = self._validate_container(dst_container)

        for blob_name in blob_names:
            src_blob = src_container.get_blob_client(blob_name)
            dst_blob = dst_container.get_blob_client(blob_name)
            dst_blob.start_copy_from_url(src_blob.url, standard_blob_tier=standard_blob_tier)
            if delete_src:
                src_blob.delete_blob()

            logger.debug(
                f"Moved {blob_name} from {src_container.container_name} to {dst_container.container_name}"
                f" and {'deleted' if delete_src else 'did not delete'} the source"
            )

    def append_to_cloud(
        self, dst_container: str | ContainerClient, src_file: Path, safe_mode: Optional[bool] = False
    ) -> bool:
        """Append a block of CSV data to an existing CSV file in the cloud

        Parameters
        ----------
        dst_container: destination container
        src_file: source file Path
        safe_mode: if safe_mode is enabled, the function checks the local and remote column headers are 
        consistent

        Return
        ------
        bool indicating whether data was successfully written to the blob

        If the remote file doesn't already exist it will be created.
        If the remote file does exist, the first line (headers) in the src_file will be dropped
        so that we don't duplicate a header row.
        It the responsibility of the calling function to ensure that the columns & headers in the
        CSV data are consistent between local and remote files"""

        try:
            dst_container = self._validate_container(dst_container)

            # Read the local file data ready to append
            with src_file.open("r") as file:
                local_lines = file.readlines()
                if len(local_lines) == 1:
                    return False  # No data beyond headers

            # Get the blob client
            blob_client = dst_container.get_blob_client(src_file.name)

            if not blob_client.exists():
                blob_client.create_append_blob()
                # Include the Headers
                data_to_append = "".join(local_lines[:])
            else:
                if safe_mode and not self._headers_match(blob_client, local_lines[0]):
                    # We bin out rather than set inconsistent fields
                    logger.error(
                        f"{root_cfg.RAISE_WARN()}Failed due to inconsistent headers: local={local_lines[0]}"
                    )
                    return False
                # Drop the Headers in the first line so we don't have repeat header rows
                data_to_append = "".join(local_lines[1:])

            # Append the data
            blob_client.append_block(data_to_append)
            return True
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Failed to append data to {blob_client.blob_name}: {e!s}")
            return False

    def container_exists(self, container: str | ContainerClient) -> bool:
        """Check if the specified container exists"""
        containerClient = self._validate_container(container)
        return containerClient.exists()

    def exists(self, src_container: str | ContainerClient, blob_name: str) -> bool:
        """Check if the specified blob exits"""
        containerClient = self._validate_container(src_container)
        blob_client = containerClient.get_blob_client(blob_name)
        return blob_client.exists()

    def delete(self, container: str | ContainerClient, blob_name: str) -> None:
        """Delete specified blob"""
        containerClient = self._validate_container(container)
        blob_client = containerClient.get_blob_client(blob_name)
        blob_client.delete_blob()

    def list_cloud_files(
        self,
        container: str | ContainerClient,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
        more_recent_than: Optional[datetime] = None,
    ) -> list[str]:
        """Similar to the Path.glob() method but against a cloud datastore.

        Parameters
        ----------
        - datastore: CloudDatastore defining the container to be searched
        - prefix: prefix to match to files in the datastore container; does not support wildcards
        - suffix: suffix to match to files in the datastore container
        - more_recent_than: Optional; if specified, only files more recent than this date will be returned

        The current backend implementation is the Azure Blobstore which only supports prefix search 
        and tag search.
        """

        containerClient = self._validate_container(container)

        files = []
        if prefix is not None:
            files = list(containerClient.list_blob_names(name_starts_with=prefix))
        else:
            files = list(containerClient.list_blob_names())

        if suffix is not None:
            files = [f for f in files if f.endswith(suffix)]

        if more_recent_than is not None:
            files = [
                f for f in files if fn.get_file_datetime(f) > more_recent_than
            ]
        logger.debug(f"list_cloud_files returning {len(files)!s} files")

        return files

    def get_blob_modified_time(self, container: str | ContainerClient, blob_name: str) -> datetime:
        """Get the last modified time of the specified blob"""
        containerClient = self._validate_container(container)
        blob_client = containerClient.get_blob_client(blob_name)
        if blob_client.exists():
            last_modified = blob_client.get_blob_properties().last_modified
            # The Azure timezone is UTC but it's not explicitly set; set it
            return last_modified.replace(tzinfo=timezone.utc)
        else:
            return datetime.min.replace(tzinfo=timezone.utc)

    ####################################################################################################
    # Private utility methods
    ####################################################################################################
    def _validate_container(self, container: str | ContainerClient) -> ContainerClient:
        if isinstance(container, str):
            return ContainerClient.from_connection_string(
                conn_str=self._get_connection_string(), container_name=container
            )
        else:
            return container

    def _get_connection_string(self) -> str:
        return self._connection_string

    def _get_lease(self, src_container: str | ContainerClient, blob_name: Path) -> object:
        download_container = self._validate_container(src_container)
        blob_client = download_container.get_blob_client(blob_name.name)
        return blob_client.acquire_lease(60)

    def _release_lease(self, lease: object) -> None:
        assert isinstance(lease, BlobLeaseClient)
        lease.release()

    def _headers_match(self, blob_client: BlobClient, local_line: str) -> bool:
        # Check the local and remote headers match
        start_of_contents = blob_client.download_blob(encoding="utf-8").read(1000)
        if start_of_contents is not None and start_of_contents != "":
            # Get the first line from start_of_contents
            cloud_lines = start_of_contents.splitlines()
            if len(cloud_lines) >= 1:
                # We have headers from local and cloud files; check headers match
                local_headers = local_line.strip().split(",")
                cloud_headers = cloud_lines[0].strip().split(",")
                if len(local_headers) != len(cloud_headers) or not all(
                    lh == ch for lh, ch in zip(local_headers, cloud_headers)
                ):
                    # They don't match
                    logger.warning(f"Local and remote headers do not match in {blob_client.blob_name}: "
                                   f"{local_headers}, {cloud_headers}")
                    return False
        # We can't find any issues
        return True
