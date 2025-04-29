import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from queue import Queue
from threading import Thread
from time import sleep
from typing import Optional

from azure.storage.blob import BlobClient, BlobLeaseClient, ContainerClient, StandardBlobTier

from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger(name="sensor_core")

class CloudType(Enum):
    """Enum for the supported cloud types"""
    AZURE = "azure"
    LOCAL_EMULATOR = "local"

#############################################################
# Blob storage tiers
#
# See Azure documentation for details:
# https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
#############################################################
class BlobTier(Enum):
    """Enum for the supported blob tiers"""
    HOT = "Hot"
    COOL = "Cool"
    COLD = "Cold"

tier_map = {
    BlobTier.HOT: StandardBlobTier.HOT,
    BlobTier.COOL: StandardBlobTier.COOL,
    BlobTier.COLD: StandardBlobTier.ARCHIVE,
}

##########################################################################################################


# Default implementation of the CloudConnector class and interface definition.
# This class is used to connect to the cloud storage provider (Azure Blob Storage) but does so 
# # synchronously.
##########################################################################################################


class CloudConnector:
    def __init__(self) -> None:
        if root_cfg.my_device is None:
            raise ValueError("System configuration not set; cannot connect to cloud")

        if root_cfg.keys is None or root_cfg.keys.cloud_storage_key == root_cfg.FAILED_TO_LOAD:
            raise ValueError("Cloud storage credentials not set; cannot connect to cloud")

        self._connection_string = root_cfg.keys.cloud_storage_key

    @staticmethod
    def get_instance(type: Optional[CloudType]=CloudType.AZURE) -> "CloudConnector":
        """We use a factory pattern to offer up alternative types of CloudConnector for accessing
        different cloud storage providers and / or the local emulator.
        
        The user should call stop() on any instance of CloudConnector before exiting the program, 
        so that all resources are cleaned up and threads exit cleanly."""
        if type == CloudType.AZURE and root_cfg.TEST_MODE == root_cfg.MODE.PRODUCTION:
            return AsyncCloudConnector()
        elif type == CloudType.LOCAL_EMULATOR or root_cfg.TEST_MODE == root_cfg.MODE.TEST:
            return LocalCloudConnector()
        else:
            raise ValueError(f"Unsupported cloud type: {type}")

    def stop(self) -> None:
        """Stop the cloud connector"""
        # Only required for the AsyncCloudConnector; the default implementation does nothing
        pass

    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: Optional[bool] = True,
        blob_tier: Enum=BlobTier.HOT,
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
                        standard_blob_tier=tier_map[BlobTier(blob_tier)],
                    )
                if delete_src:
                    logger.debug(f"Deleting uploaded file: {file}")
                    file.unlink()
            else:
                logger.error(f"{root_cfg.RAISE_WARN()}Upload failed because file {file} does not exist")

    def download_from_container(
        self, src_container: str, src_file: str, dst_file: Path
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
        src_container: str,
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

    def move_between_containers(
        self,
        src_container: str,
        dst_container: str,
        blob_names: list[str],
        delete_src: bool = False,
        blob_tier: Enum =BlobTier.COOL,
    ) -> None:
        """Move blobs between containers

        Parameters
        ----------
        src_container: source container
        dst_container: destination container
        blob_names: list of blob names to move
        delete_src: delete the source blobs after successful upload; defaults to False
        """
        from_container = self._validate_container(src_container)
        to_container = self._validate_container(dst_container)

        for blob_name in blob_names:
            src_blob = from_container.get_blob_client(blob_name)
            dst_blob = to_container.get_blob_client(blob_name)
            dst_blob.start_copy_from_url(src_blob.url, 
                                         standard_blob_tier=tier_map[BlobTier(blob_tier)])
            if delete_src:
                src_blob.delete_blob()

            logger.debug(
                f"Moved {blob_name} from {from_container.container_name} to {to_container.container_name}"
                f" and {'deleted' if delete_src else 'did not delete'} the source"
            )

    def append_to_cloud(self, 
                        dst_container: str, 
                        src_file: Path, 
                        delete_src: bool, 
                        safe_mode: Optional[bool] = False,
                        orig_name: Optional[str] = None,
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
            logger.debug(f"CloudConnector.append_to_cloud() with delete_src={delete_src} for {src_file}")
            target_container = self._validate_container(dst_container)

            # Read the local file data ready to append
            with src_file.open("r") as file:
                local_lines = file.readlines()
                if len(local_lines) == 1:
                    return False  # No data beyond headers

            # Get the blob client
            if orig_name is not None:
                blob_client = target_container.get_blob_client(orig_name)
            else:
                blob_client = target_container.get_blob_client(src_file.name)

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
            if delete_src:
                logger.debug(f"Deleting append file: {src_file}")
                src_file.unlink()

            return True
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Failed to append data to {blob_client.blob_name}: {e!s}")
            return False

    def container_exists(self, container: str) -> bool:
        """Check if the specified container exists"""
        containerClient = self._validate_container(container)
        return containerClient.exists()

    def exists(self, src_container: str, blob_name: str) -> bool:
        """Check if the specified blob exits"""
        containerClient = self._validate_container(src_container)
        blob_client = containerClient.get_blob_client(blob_name)
        return blob_client.exists()

    def delete(self, container: str, blob_name: str) -> None:
        """Delete specified blob"""
        containerClient = self._validate_container(container)
        blob_client = containerClient.get_blob_client(blob_name)
        blob_client.delete_blob()

    def list_cloud_files(
        self,
        container: str,
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
                f for f in files if file_naming.get_file_datetime(f) > more_recent_than
            ]
        logger.debug(f"list_cloud_files returning {len(files)!s} files")

        return files

    def get_blob_modified_time(self, container: str, blob_name: str) -> datetime:
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
    def _download_file(self, src: BlobClient, dst_file: Path) -> str:
        """Download a single file"""

        if not dst_file.parent.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)
        with open(dst_file, "wb") as my_file:
            download_stream = src.download_blob()
            my_file.write(download_stream.readall())
        return dst_file.name


    def _validate_container(self, container: str) -> ContainerClient:
        if isinstance(container, str):
            return ContainerClient.from_connection_string(
                conn_str=self._get_connection_string(), container_name=container
            )
        else:
            return container

    def _get_connection_string(self) -> str:
        return self._connection_string

    def _get_lease(self, src_container: ContainerClient, blob_name: Path) -> object:
        blob_client = src_container.get_blob_client(blob_name.name)
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


#########################################################################################################
# LocalCloudConnector class
#
# This class is used to connect to the local cloud emulator.  It is a subclass of CloudConnector and
# implements the same interface.  It is used for testing purposes only and should not be used in production.
#########################################################################################################
local_cloud_root = root_cfg.ROOT_WORKING_DIR / "local_cloud"
local_cloud: Path = local_cloud_root / api.utc_to_fname_str()

class LocalCloudConnector(CloudConnector):
    def __init__(self) -> None:
        if root_cfg.my_device is None:
            raise ValueError("System configuration not set; cannot connect to cloud")
        self.local_cloud = local_cloud
        
    def get_local_cloud(self) -> Path:
        """Creates a local cloud directory.  Usually called by ScEmulator.__enter__() as
        when the ScEmulator is used as a context manager.

        This is an unpredictable string so we don't clash with other local cloud instances."""
        if self.local_cloud.exists():
            shutil.rmtree(self.local_cloud)
        self.local_cloud.mkdir(parents=True, exist_ok=True)
        return self.local_cloud

    def clear_local_cloud(self):
        """Clear the local cloud storage - this is used for testing only"""
        if self.local_cloud.exists():
            shutil.rmtree(self.local_cloud)

    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: Optional[bool] = True,
        standard_blob_tier: Enum=BlobTier.HOT,
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
        for file in src_files:
                if file.exists():
                    # Copy the file to the local cloud directory
                    dst_file = self.local_cloud / dst_container / file.name
                    dst_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(file, dst_file)
                    if delete_src:
                        file.unlink()

    def download_from_container(
        self, src_container: str, src_file: str, dst_file: Path
    ) -> None:
        """Downloads the src_file to a local dst_file Path."""

        if dst_file is None or not isinstance(dst_file, Path):
            return

        if not dst_file.parent.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)

        if dst_file.exists():
            dst_file.unlink()

        shutil.copy(self.local_cloud / src_container / src_file, dst_file)

    def download_container(
        self,
        src_container: str,
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

        download_container = self.local_cloud / src_container

        if files is None:
            for blob in download_container.glob("*"):
                if folder_prefix_len is not None:
                    prefix_folder_dir = dst_dir.joinpath(blob.name[:folder_prefix_len])
                    if not prefix_folder_dir.exists():
                        prefix_folder_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(blob, prefix_folder_dir)
        else:
            for blob_name in files:
                src_file = download_container / blob_name
                if folder_prefix_len is not None:
                    prefix_folder_dir = dst_dir.joinpath(blob_name[:folder_prefix_len])
                dst_file = prefix_folder_dir.joinpath(blob_name)
                if not overwrite and dst_file.exists():
                    logger.debug(f"File {dst_file} already exists; skipping download")
                    continue
                shutil.copy(src_file, dst_file)

    def move_between_containers(
        self,
        src_container: str,
        dst_container: str,
        blob_names: list[str],
        delete_src: bool = False,
        standard_blob_tier: Enum =BlobTier.COOL,
    ) -> None:
        """Move blobs between containers

        Parameters
        ----------
        src_container: source container
        dst_container: destination container
        blob_names: list of blob names to move
        delete_src: delete the source blobs after successful upload; defaults to False
        """
        for blob_name in blob_names:
            shutil.copy(
                self.local_cloud / src_container / blob_name,
                self.local_cloud / dst_container / blob_name
            )
            if delete_src:
                (self.local_cloud / src_container / blob_name).unlink()

            logger.debug(
                f"Moved {blob_name} from {src_container} to {dst_container}"
                f" and {'deleted' if delete_src else 'did not delete'} the source"
            )

    def append_to_cloud(self, 
                        dst_container: str, 
                        src_file: Path, 
                        delete_src: bool, 
                        safe_mode: Optional[bool] = False,
                        orig_name: Optional[str] = None,
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
            logger.debug(f"LocalCC.append_to_cloud() with delete_src={delete_src} for {src_file}")

            # Read the local file data ready to append
            with src_file.open("r") as file:
                local_lines = file.readlines()
                if len(local_lines) == 1:
                    return False  # No data beyond headers

            # Get the blob client
            blob_client = self.local_cloud / dst_container / src_file.name

            if not blob_client.exists():
                # Include the Headers
                data_to_append = "".join(local_lines[:])
                # Create the file
                blob_client.parent.mkdir(parents=True, exist_ok=True)
                blob_client.touch()
            else:
                # Drop the Headers in the first line so we don't have repeat header rows
                data_to_append = "".join(local_lines[1:])

            # Append the data to the local file
            with blob_client.open("a") as blob_file:
                blob_file.write(data_to_append)

            if delete_src:
                logger.debug(f"Deleting append file: {src_file}")
                src_file.unlink()

            return True
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Failed to append data to {blob_client}: {e!s}")
            return False

    def container_exists(self, container: str) -> bool:
        """Check if the specified container exists"""
        # We always return true in the emulator; creating the container if it doesn't exist
        containerClient = self.local_cloud / container
        if not containerClient.exists():
            containerClient.mkdir(parents=True, exist_ok=True)
        return True

    def exists(self, src_container: str, blob_name: str) -> bool:
        """Check if the specified blob exits"""
        blob_client = self.local_cloud / src_container / blob_name
        return blob_client.exists()

    def delete(self, container: str, blob_name: str) -> None:
        """Delete specified blob"""
        blob_client = self.local_cloud / container / blob_name
        blob_client.unlink()

    def list_cloud_files(
        self,
        container: str,
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
        containerClient = self.local_cloud / container

        files = file_paths = []
        if prefix is not None:
            query = f"{prefix}*"
        else:
            query = "*"
        file_paths = list(containerClient.glob(query))
        files = [f.name for f in file_paths]

        if suffix is not None:
            files = [f for f in files if f.endswith(suffix)]

        if more_recent_than is not None:
            files = [
                f for f in files if file_naming.get_file_datetime(f) > more_recent_than
            ]
        logger.debug(f"list_cloud_files returning {len(files)!s} files")

        return files

    def get_blob_modified_time(self, container: str, blob_name: str) -> datetime:
        """Get the last modified time of the specified blob"""
        containerClient = self.local_cloud / container
        blob_client = containerClient / blob_name
        if blob_client.exists():
            last_modified = blob_client.stat().st_mtime
            # The Azure timezone is UTC but it's not explicitly set; set it
            return datetime.fromtimestamp(last_modified, tz=timezone.utc)
        else:
            return datetime.min.replace(tzinfo=timezone.utc)

#####################################################################################################
# AsyncCloudConnector class
# This class uses async methods to *UPLOAD* files to the cloud storage provider (Azure Blob Storage).
# This improves resilience to transient network issues and reduces data loss.
# Download / exists / list methods are *not* asynchronous and use the default CloudConnector.
#####################################################################################################
class AsyncCloudConnector(Thread, CloudConnector):
    class METHOD(Enum):
        """Enum for the supported methods"""
        UPLOAD = "upload"
        APPEND = "append"

    def __init__(self) -> None:
        Thread.__init__(self)
        CloudConnector.__init__(self)
        self._stop_requested = False
        self._upload_queue: Queue = Queue()
        self.start()

    #################################################################################################
    # Public methods
    #################################################################################################
    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: Optional[bool] = True,
        blob_tier: Enum = BlobTier.HOT,
    ) -> None:
        """
        Async version of upload_to_container using a queue and thread pool for parallel uploads.
        """
        if not isinstance(src_files, list):
            src_files = [src_files]

        for file in src_files:
            if not file.exists():
                logger.error(f"{root_cfg.RAISE_WARN()}Upload of file {file} aborted; does not exist")
                src_files.remove(file)
        
        if src_files:
            self._upload_queue.put((AsyncCloudConnector.METHOD.UPLOAD, 
                                    dst_container,
                                    src_files, 
                                    delete_src, 
                                    blob_tier,
                                    0,
                                    None))

    def append_to_cloud(self, 
                        dst_container: str, 
                        src_file: Path, 
                        delete_src: bool, 
                        safe_mode: Optional[bool] = False,
                        orig_name: Optional[str] = None,
    ) -> bool:
        """
        Async version of append_to_cloud.
        """
        logger.debug(f"AyncCC.append_to_cloud() with delete_src={delete_src} for {src_file}")
        if isinstance(src_file, str):
            src_file = Path(src_file)
        
        if not src_file.exists():
            logger.error(f"{root_cfg.RAISE_WARN()}Upload failed because file {src_file} does not exist")
            return False
        
        # Although this is asynchronous, we need to appear to delete the src_files synchronously to 
        # avoid issues with fixed-name journals.  However we need the original name in order to append
        # it to the correct cloud file.  We therefore create a mapping from temporary filename to 
        # cloud file name.
        # We change the file name to this temporary name and then delete it after the upload.
        
        # Create a temporary file name for each src_file
        if delete_src:
            tmp_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
            src_file.rename(tmp_file)
        else:
            tmp_file = src_file

        self._upload_queue.put((AsyncCloudConnector.METHOD.APPEND, 
                                dst_container,
                                [tmp_file],
                                delete_src, 
                                BlobTier.HOT,
                                0,
                                [src_file.name]))

        return True

    def stop(self) -> None:
        """Stop the cloud connector"""
        # Stop the executor and process the queue
        self._stop_requested = True

        # Trigger the ThreadPoolExecutor to stop
        self._upload_queue.put((None, None, None, None, None, None, None))
        self.join()


    ##################################################################################################
    # Private methods
    ##################################################################################################
    def _async_upload_method(
        self,
        method: METHOD,
        dst_container: str,
        src_files: list[Path],
        delete_src: bool,
        blob_tier: Enum = BlobTier.HOT,
        iteration: int = 0,
        orig_names: Optional[list[str]] = None,
    ) -> None:
        """A wrapper to handle failure when uploading a file to the cloud asynchronously.
        We re-queue the upload if it fails if the src files still exist.
        This method is called on a thread from the ThreadPoolExecutor."""
        try:
            logger.debug(f"_async {method} with delete_src={delete_src}, "
                         f"iteration {iteration} for {src_files}")
            if method == AsyncCloudConnector.METHOD.UPLOAD:
                super().upload_to_container(dst_container, src_files, delete_src, blob_tier)
            elif method == AsyncCloudConnector.METHOD.APPEND:
                orig_name = orig_names[0] if orig_names is not None else None
                super().append_to_cloud(dst_container, 
                                        src_files[0], 
                                        delete_src=delete_src, 
                                        orig_name=orig_name)
        except Exception as e:
            # Check all the src_files still exist and drop any that don't
            logger.warning(f"Upload failed for {src_files} on iter {iteration}: {e!s}")
            for file in src_files:
                if not file.exists():
                    src_files.remove(file)

            if src_files:
                # Re-queue the upload if any src_files still exist
                self._upload_queue.put((method, 
                                        dst_container,
                                        src_files, 
                                        delete_src, 
                                        blob_tier,
                                        iteration + 1,
                                        orig_names))
                # Back off for a bit before re-trying the upload
                sleep(2 * iteration)

    def run(self) -> None:
        """Use a ThreadPoolExecutor to process the upload queue."""

        with ThreadPoolExecutor(max_workers=8) as executor:
            while not self._stop_requested:
                method, dst_container, files, delete_src, blob_tier, iteration, orig_names = \
                    self._upload_queue.get()
                executor.submit(self._async_upload_method,
                                method, 
                                dst_container, 
                                files, 
                                delete_src,
                                blob_tier,
                                iteration,
                                orig_names)
                self._upload_queue.task_done()

        logger.info("Upload queue processing stopped")


