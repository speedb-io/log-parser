import copy
import logging
from dataclasses import dataclass
from enum import Enum, auto

import events
import utils


@dataclass
class DbFileInfo:
    file_number: int
    cf_name: str
    creation_time: str
    deletion_time: str = None
    # TODO - Remove
    size_bytes: int = 0
    compressed_file_size_bytes: int = 0
    compressed_data_size_bytes: int = 0
    data_size_bytes: int = 0
    index_size_bytes: int = 0
    filter_size_bytes: int = 0
    filter_policy: str = None
    num_filter_entries: int = 0
    compression_type: str = None
    level: int = None

    creation_event: events.Event = None
    deletion_event: events.Event = None

    def is_alive(self):
        return self.deletion_time is None

    def was_created_in_log(self):
        return self.creation_time is not None

    def was_deleted_without_creation(self):
        return not self.was_created_in_log()

    def file_deleted(self, deletion_time):
        assert self.is_alive()
        self.deletion_time = deletion_time


class BlockType(Enum):
    INDEX = auto()
    FILTER = auto()
    DATA = auto()


class BlockLiveFileStats:
    def __init__(self):
        # TODO - Remove
        self.num_created = 0
        # TODO - Remove
        self.num_live = 0
        # Total size of all the blocks in created files (never decreasing)
        self.total_created_size_bytes = 0

        # Current total size of live blocks of this type
        self.curr_total_live_size_bytes = 0

        # The size of the largest block created and its creation time
        self.largest_block_size_bytes = 0
        self.largest_block_size_time = None

        # The max total live size at some point in time
        self.max_total_live_size_bytes = 0
        self.max_total_live_size_time = None

    def __eq__(self, other):
        assert isinstance(other, BlockLiveFileStats)

        return self.num_created == other.num_created and \
            self.num_live == other.num_live and \
            self. total_created_size_bytes == \
            other.total_created_size_bytes and \
            self.curr_total_live_size_bytes == \
            other.curr_total_live_size_bytes and \
            self.largest_block_size_bytes == \
            other.largest_block_size_bytes and \
            self.largest_block_size_time == other.largest_block_size_time
        # These are incorrect when there is more than a single cf
        # and \
        # self.max_total_live_size_bytes == \
        # other.max_total_live_size_bytes and \
        # self.max_total_live_size_time == other.max_total_live_size_time

    def block_created(self, size_bytes, time):
        if size_bytes == 0:
            return

        self.num_created += 1
        self.num_live += 1
        self.total_created_size_bytes += size_bytes
        self.curr_total_live_size_bytes += size_bytes

        if self.largest_block_size_bytes < size_bytes:
            self.largest_block_size_bytes = size_bytes
            self.largest_block_size_time = time

        if self.max_total_live_size_bytes <\
                self.curr_total_live_size_bytes:
            self.max_total_live_size_bytes = \
                self.curr_total_live_size_bytes
            self.max_total_live_size_time = time

    def block_deleted(self, size_bytes):
        if size_bytes == 0:
            return

        assert self.num_live > 0
        assert self.num_live <= self.num_created
        self.num_live -= 1

        assert self.curr_total_live_size_bytes >= \
               size_bytes
        self.curr_total_live_size_bytes -= size_bytes

    def get_avg_block_size(self):
        if self.num_created == 0:
            return 0
        return self.total_created_size_bytes / self.num_created


class DbLiveFilesStats:
    def __init__(self):
        self.num_created = 0
        self.num_live = 0

        self.blocks_stats = {
            BlockType.DATA: BlockLiveFileStats(),
            BlockType.INDEX: BlockLiveFileStats(),
            BlockType.FILTER: BlockLiveFileStats()
        }

    def file_created(self, file_info):
        assert isinstance(file_info, DbFileInfo)

        self.num_created += 1
        self.num_live += 1
        self.blocks_stats[BlockType.DATA].block_created(
            file_info.data_size_bytes, file_info.creation_time)
        self.blocks_stats[BlockType.INDEX].block_created(
            file_info.index_size_bytes, file_info.creation_time)
        self.blocks_stats[BlockType.FILTER].block_created(
            file_info.filter_size_bytes, file_info.creation_time)

    def file_deleted(self, file_info):
        assert isinstance(file_info, DbFileInfo)

        assert self.num_live > 0
        assert self.num_live <= self.num_created
        self.num_live -= 1

        self.blocks_stats[BlockType.DATA].block_deleted(
            file_info.data_size_bytes)
        self.blocks_stats[BlockType.INDEX].block_deleted(
            file_info.index_size_bytes)
        self.blocks_stats[BlockType.FILTER].block_deleted(
            file_info.filter_size_bytes)


class DbFilesMonitor:
    def __init__(self):
        # Format: {<file_number>
        self.files = dict()
        self.live_files_stats = dict()
        self.cfs_names = list()

    def new_event(self, event):
        event_type = event.get_type()
        if event_type == events.EventType.TABLE_FILE_CREATION:
            return self.file_created(event)
        elif event_type == events.EventType.TABLE_FILE_DELETION:
            return self.file_deleted(event)

        return True

    def file_created(self, event):
        assert isinstance(event, events.TableFileCreationEvent)

        file_number = event.get_created_file_number()
        if file_number in self.files:
            logging.error(
                f"Created File (number {file_number}) already exists, "
                f"Ignoring Event.\n{event}")
            return False

        cf_name = event.get_cf_name()
        file_info = DbFileInfo(file_number,
                               cf_name=cf_name,
                               creation_time=event.get_log_time())
        file_info.creation_event = event
        file_info.compressed_file_size_bytes = \
            event.get_compressed_file_size_bytes()
        file_info.compressed_data_size_bytes = \
            event.get_compressed_data_size_bytes()
        file_info.data_size_bytes = event.get_data_size_bytes()
        file_info.index_size_bytes = event.get_index_size_bytes()
        file_info.filter_size_bytes = event.get_filter_size_bytes()
        file_info.compression_type = event.get_compression_type()

        if event.does_use_filter():
            file_info.filter_policy = event.get_filter_policy()
            file_info.num_filter_entries = event.get_num_filter_entries()

        self.files[file_number] = file_info

        if cf_name not in self.cfs_names:
            self.cfs_names.append(cf_name)
            assert cf_name not in self.live_files_stats
            self.live_files_stats[cf_name] = DbLiveFilesStats()

        self.live_files_stats[cf_name].file_created(file_info)

        return True

    def file_deleted(self, event):
        assert isinstance(event, events.TableFileDeletionEvent)

        file_number = event.get_deleted_file_number()
        if file_number not in self.files:
            logging.info(f"File deleted event ((number {file_number}) "
                         f"without a previous file created event, Ignoring "
                         f"Event.\n{event}")
            return False

        file_info = self.files[file_number]
        if not file_info.is_alive():
            logging.error(
                f"Deleted File (number {file_number}) already "
                f"deleted, Ignoring Event.\n{event}")
            return False

        file_info.deletion_event = event
        file_info.file_deleted(event.get_log_time())
        assert file_info.cf_name in self.live_files_stats
        self.live_files_stats[file_info.cf_name].file_deleted(file_info)

        return True

    def get_all_files(self, file_filter=lambda info: True):
        # Returns {<cf>: [<live files for this cf>
        files = {}
        for info in self.files.values():
            if file_filter(info):
                if info.cf_name not in files:
                    files[info.cf_name] = list()
                files[info.cf_name].append(info)
        return files

    def get_all_files_flat(self):
        return self.files

    def get_all_cf_files(self, cf_name):
        all_cf_files = \
            self.get_all_files(lambda info: info.cf_name == cf_name)
        if not all_cf_files:
            return []
        return all_cf_files[cf_name]

    def get_all_live_files(self):
        return self.get_all_files(lambda info: info.is_alive())

    def get_cf_live_files(self, cf_name):
        cf_live_files = \
            self.get_all_files(
                lambda info: info.is_alive() and info.cf_name == cf_name)
        if not cf_live_files:
            return []
        return cf_live_files[cf_name]

    def get_blocks_stats(self):
        stats = {}
        for cf_name, cf_blocks_stats in self.live_files_stats.items():
            stats[cf_name] = {
                block_type: block_stats for block_type, block_stats in
                cf_blocks_stats.blocks_stats.items()}
        return stats

    def get_cfs_names(self):
        return self.cfs_names


#
# Files Stats
#

@dataclass
class CfFilterSpecificStats:
    filter_policy: str = None
    avg_bpk: float = 0.0


class CfsFilesStats:
    def __init__(self):
        self.blocks_stats = \
            {block_type: BlockLiveFileStats() for block_type in BlockType}

        # {<cf_name>: FilterSpecificStats}
        self.cfs_filter_specific = dict()


def get_block_stats_for_cfs_group(cfs_names, files_monitor, block_type):
    assert isinstance(files_monitor, DbFilesMonitor)
    assert isinstance(block_type, BlockType)

    blocks_stats_all_cfs = files_monitor.get_blocks_stats()

    stats = None
    for cf_name in cfs_names:
        if cf_name not in blocks_stats_all_cfs:
            continue

        assert block_type in blocks_stats_all_cfs[cf_name]
        if stats is None:
            stats = copy.deepcopy(blocks_stats_all_cfs[cf_name][block_type])
            assert isinstance(stats, BlockLiveFileStats)
        else:
            block_stats = blocks_stats_all_cfs[cf_name][block_type]
            assert isinstance(block_stats, BlockLiveFileStats)
            stats.num_created += block_stats.num_created
            stats.num_live += block_stats.num_live
            stats.total_created_size_bytes += \
                block_stats.total_created_size_bytes
            stats.curr_total_live_size_bytes += \
                block_stats.curr_total_live_size_bytes
            if stats.largest_block_size_bytes < \
                    block_stats.largest_block_size_bytes:
                stats.largest_block_size_bytes = \
                    block_stats.largest_block_size_bytes
                stats.largest_block_size_time = \
                    block_stats.largest_block_size_time
            stats.max_total_live_size_bytes += \
                block_stats.max_total_live_size_bytes

    return stats


def calc_cf_files_stats(cfs_names, files_monitor):
    assert isinstance(files_monitor, DbFilesMonitor)

    stats = CfsFilesStats()

    for block_type in BlockType:
        stats.blocks_stats[block_type] = get_block_stats_for_cfs_group(
            cfs_names, files_monitor, block_type)

    num_cf_files = 0
    for cf_name in cfs_names:
        cf_files = files_monitor.get_all_cf_files(cf_name)
        num_cf_files += len(cf_files) if cf_files else 0

        if num_cf_files == 0:
            logging.info(f"No files for cf {cf_name}")
            continue

        total_filters_sizes_bytes = 0
        total_num_filters_entries = 0
        filter_policy = None
        for i, file_info in enumerate(cf_files):
            if i == 0:
                # Set the filter policy for the cf on the first. Changing
                # it later is either a bug, or an unsupported (by the
                # parser) dynamic change of the filter policy
                filter_policy = file_info.filter_policy
            else:
                if filter_policy != utils.INVALID_FILTER_POLICY \
                        and filter_policy != file_info.filter_policy:
                    logging.warning(
                        f"Filter policy changed for CF. Not supported"
                        f"currently. cf:{cf_name}\nfile_info:{file_info}")
                    filter_policy = utils.INVALID_FILTER_POLICY
                    continue
            total_filters_sizes_bytes += file_info.filter_size_bytes
            total_num_filters_entries += file_info.num_filter_entries

        avg_bpk = 0
        if filter_policy is not None and filter_policy != \
                utils.INVALID_FILTER_POLICY:
            if total_num_filters_entries > 0:
                avg_bpk =\
                    (8 * total_filters_sizes_bytes) / total_num_filters_entries
            else:
                logging.warning(f"No filter entries. cf:{cf_name}\n"
                                f"file info:{file_info}")
        stats.cfs_filter_specific[cf_name] =\
            CfFilterSpecificStats(filter_policy=filter_policy, avg_bpk=avg_bpk)

    if num_cf_files == 0:
        logging.info(f"No files for all cf-s ({[cfs_names]}")
        return None

    return stats
