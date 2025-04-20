from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock
from typing import Optional

import pandas as pd

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import DatastreamCfg
from sensor_core.configuration import Mode
from sensor_core.utils import file_naming
from sensor_core.utils.cloud_journal import CloudJournal
from sensor_core.utils.journal import Journal

logger = root_cfg.setup_logger("sensor_core")


class JournalPool(ABC):
    """The JournalPool is responsible for thread-safe saving of data to journal files, and onwards to archive.

    The JournalPool is a singleton shared by all Datastream instances and should be retrieved using 
    JournalPool.get().

    The internal implementation of the JournalPool is different for EDGE and ETL modes.
    On the EDGE, there is a "Journal" in the JournalPool per DatastreamType.
    On the ETL, we use "CloudJournal" objects to manage the data, with 1 Journal per DatastreamType per day.
    In both cases, data is stored based on its bapi.RECORD_ID.DS_TYPE_ID and bapi.RECORD_ID.TIMESTAMP in the 
    appropriate CJ.
    """

    _instance: Optional[JournalPool] = None

    @staticmethod
    def get(mode: Mode) -> JournalPool:
        """Get the singleton instance of the JournalPool"""
        if JournalPool._instance is None:
            if mode is None:
                raise ValueError("Datastream mode has not been set.")
            # We create different concrete implementations depending on the mode we're running in
            if mode == Mode.EDGE:
                JournalPool._instance = CloudJournalPool() # @@@ LocalJournalPool()
            else:
                JournalPool._instance = CloudJournalPool()
        return JournalPool._instance

    @abstractmethod
    def add_rows(self, ds: DatastreamCfg, data: list[dict], timestamp: Optional[datetime] = None) -> None:
        """Add data rows as a list of dictionaries

        The fields in each dictionary must match the datastream reqd_fields."""

        assert False, "Abstract method needs to be implemented"

    @abstractmethod
    def add_rows_from_df(
        self, ds: DatastreamCfg, data: pd.DataFrame, timestamp: Optional[datetime] = None
    ) -> None:
        """Add data in the form of a Pandas DataFrame to the Journal, which will auto-sync to the cloud

        All data MUST relate to the same DAY as timestamp."""

        assert False, "Abstract method needs to be implemented"

    @abstractmethod
    def flush_journals(self) -> None:
        """Flush all journals to disk and onwards to archive"""

        assert False, "Abstract method needs to be implemented"

    @abstractmethod
    def stop(self) -> None:
        """Stop the JournalPool, flush all data and exit any threads"""

        assert False, "Abstract method needs to be implemented"

class CloudJournalPool(JournalPool):
    """The CloudJournalPool is a concrete implementation of a JournalPool for running in ETL mode.

    It is based on a pool of CloudJournal instances.
    """

    def __init__(self) -> None:
        self._cj_pool: dict[str, CloudJournal] = {}
        self.jlock = Lock()

    def add_rows(self, ds: DatastreamCfg, data: list[dict], timestamp: Optional[datetime] = None) -> None:
        """Add data to the appropriate CloudJournal, which will auto-sync to the cloud

        All data MUST relate to the same DAY as timestamp."""

        assert timestamp is not None, "Timestamp must be provided for add_rows_from_df with CloudJournalPool"
        with self.jlock:
            cj = self._get_journal(ds, timestamp)
            cj.add_rows(data)

    def add_rows_from_df(
        self, ds: DatastreamCfg, data: pd.DataFrame, timestamp: Optional[datetime] = None
    ) -> None:
        """Add data to the appropriate CloudJournal, which will auto-sync to the cloud

        All data MUST relate to the same DAY as timestamp."""

        assert timestamp is not None, "Timestamp must be provided for add_rows_from_df with CloudJournalPool"
        with self.jlock:
            cj = self._get_journal(ds, timestamp)
            cj.add_rows_from_df(data)

    def flush_journals(self) -> None:
        """Flush all journals to disk and onwards to archive"""
        with self.jlock:
            # We can call flush_all on any CloudJournal in the pool and all will get flushed
            for cj in self._cj_pool.values():
                cj.flush_all()
                break

    def stop(self) -> None:
        """Stop the CloudJournalPool, flush all data and exit any threads"""
        with self.jlock:
            # We can call stop on any CloudJournal in the pool and all will get stopped
            for cj in self._cj_pool.values():
                cj.stop()
                break

    def _get_journal(self, ds: DatastreamCfg, day: datetime) -> CloudJournal:
        """Generate the CloudJournal filename for a Datastream.

        The V3 filename format is:
            V3_{datastream_type_id}_{day}.csv
        """
        # Check that the archived_fields contain at least all the bapi.REQD_RECORD_ID_FIELDS
        assert ds.archived_fields is not None, f"archived_fields must be set in {ds.ds_type_id}"

        fname = file_naming.get_cloud_journal_filename(ds.ds_type_id, day)

        if fname.name not in self._cj_pool:
            # Users can choose a cloud_container per DS or use the default one
            if ds.cloud_container is None:
                ds.cloud_container = root_cfg.my_device.cc_for_journals
            cj = CloudJournal(fname, ds.cloud_container, ds.archived_fields)
            self._cj_pool[fname.name] = cj
        else:
            cj = self._cj_pool[fname.name]
        return cj


class LocalJournalPool(JournalPool):
    """The LocalJournalPool is a concrete implementation of a JournalPool for running in EDGE mode.

    It is based on a pool of Journal instances.
    """

    def __init__(self) -> None:
        self._jpool: dict[str, Journal] = {}
        self.jlock = Lock()

    def add_rows(self, ds: DatastreamCfg, data: list[dict], timestamp: Optional[datetime] = None) -> None:
        """Add data to the appropriate Journal, which will auto-upload to the cloud"""

        with self.jlock:
            j = self._get_journal(ds)
            j.add_rows(data)

    def add_rows_from_df(
        self, ds: DatastreamCfg, data: pd.DataFrame, timestamp: Optional[datetime] = None
    ) -> None:
        """Add data to the appropriate Journal, which will auto-sync to the cloud"""

        with self.jlock:
            j = self._get_journal(ds)
            j.add_rows_from_df(data)

    def flush_journals(self) -> None:
        """Called by the EdgeOrchestrator.upload_to_container function to flush all journals to disk and 
        onwards to archive"""

        logger.debug("Flushing all journals to disk")

        with self.jlock:
            for j in self._jpool.values():
                # Save the cached data to disk
                fname = j.save()

                # If there was no data, no file will have been created
                if not fname.exists():
                    logger.info(f"No data in {j.fname} when flushed")
                    continue

                # Move the file to the upload directory and append the timestamp
                ts = api.utc_to_fname_str()
                # We need a unique filename, so we append a number if the file already exists
                # We only hit this in testing, because there's usually half an hour between runs!
                i = 1
                target_fname = root_cfg.EDGE_UPLOAD_DIR.joinpath(f"{fname.stem}_{ts}{fname.suffix}")
                while target_fname.exists():
                    target_fname.replace(
                        target_fname.parent.joinpath(f"{target_fname.stem}{i!s}{target_fname.suffix}")
                    )
                    i += 1

                fname.rename(target_fname)
                # Delete the cached data from the Journal object
                j.delete()

    def stop(self) -> None:
        """Stop the LocalJournalPool, flush all data and exit any threads"""
        self.flush_journals()
        # No threads to stop

    def _get_journal(self, ds: DatastreamCfg) -> Journal:
        """Generate the Journal filename for a Datastream."""
        assert ds.archived_fields is not None, f"archived_fields must be set in {ds.ds_type_id}"

        fname = file_naming.get_journal_filename(ds.ds_type_id)
        if fname.name not in self._jpool:
            j = Journal(fname, cached=True, reqd_columns=ds.raw_fields)
            self._jpool[fname.name] = j
        else:
            j = self._jpool[fname.name]

        return j
