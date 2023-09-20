# Copyright (C) 2023 Speedb Ltd. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.'''

import copy
import logging
from bisect import bisect
from dataclasses import dataclass, field

import db_files
import db_options
import utils
from counters import CountersMngr
from events import EventType
from events import FlowType, MatchingEventInfo, EventsMngr
from log_file import ParsedLog
from stats_mngr import CompactionStatsMngr, CfFileHistogramStatsMngr, \
    DbWideStatsMngr
from warnings_mngr import WarningType, WarningsMngr, WarningElementInfo


def get_db_size_bytes_at_start(compaction_stats_mngr):
    assert isinstance(compaction_stats_mngr, CompactionStatsMngr)

    first_entry_all_cfs = compaction_stats_mngr.get_first_level_entry_all_cfs()
    if not first_entry_all_cfs:
        return 0

    size_bytes = 0
    for cf_name, cf_entry in first_entry_all_cfs.items():
        size_bytes += \
            int(CompactionStatsMngr.get_sum_value(
                cf_entry, CompactionStatsMngr.LevelFields.SIZE_BYTES))


@dataclass
class DbSizeBytesInfo:
    size_bytes: int = None
    size_time: str = None


def get_db_size_bytes_info_at_end(cfs_names, compaction_stats_mngr):
    assert isinstance(compaction_stats_mngr, CompactionStatsMngr)

    last_entry_all_cfs = compaction_stats_mngr.get_last_level_entry_all_cfs()
    if not last_entry_all_cfs:
        return DbSizeBytesInfo(size_bytes=None, size_time=None)

    size_bytes = 0
    size_time = None
    for cf_name in cfs_names:
        if cf_name not in last_entry_all_cfs:
            continue
        cf_entry = last_entry_all_cfs[cf_name]
        if size_time is None:
            size_time, _ = utils.get_last_dict_entry_components(cf_entry)

        size_bytes += \
            int(CompactionStatsMngr.get_sum_value(
                cf_entry, CompactionStatsMngr.LevelFields.SIZE_BYTES))

    return DbSizeBytesInfo(size_time=size_time, size_bytes=size_bytes)


def get_per_cf_per_level_size_bytes(entry_all_cfs):
    if not entry_all_cfs:
        return {}

    growth = dict()
    for cf_name, cf_entry in entry_all_cfs.items():
        size_bytes_per_level = \
            CompactionStatsMngr.get_field_value_for_all_levels(
                cf_entry, CompactionStatsMngr.LevelFields.SIZE_BYTES)

        growth[cf_name] = size_bytes_per_level

    return growth


def calc_cfs_size_bytes_growth(cfs_names, compaction_stats_mngr):
    assert isinstance(compaction_stats_mngr, CompactionStatsMngr)

    growth = {cf_name: None for cf_name in cfs_names}

    first_entry_all_cfs = compaction_stats_mngr.get_first_level_entry_all_cfs()
    if not first_entry_all_cfs:
        return {}

    start_per_cf_and_level_sizes_bytes = \
        get_per_cf_per_level_size_bytes(first_entry_all_cfs)

    last_entry_all_cfs = compaction_stats_mngr.get_last_level_entry_all_cfs()
    assert last_entry_all_cfs

    start_cf_names = list(start_per_cf_and_level_sizes_bytes.keys())
    for cf_name in start_cf_names:
        growth[cf_name] = {}
        start_cf_level_sizes = start_per_cf_and_level_sizes_bytes[cf_name]
        if start_cf_level_sizes:
            for level in start_cf_level_sizes:
                growth[cf_name][level] = (start_cf_level_sizes[level], None)

    end_per_cf_and_level_sizes_bytes = \
        get_per_cf_per_level_size_bytes(last_entry_all_cfs)
    end_cf_names = list(end_per_cf_and_level_sizes_bytes.keys())
    for cf_name in end_cf_names:
        if cf_name not in growth:
            growth[cf_name] = {}
        if growth[cf_name] is None:
            growth[cf_name] = {}
        end_cf_level_sizes = end_per_cf_and_level_sizes_bytes[cf_name]
        if end_cf_level_sizes:
            for level in end_cf_level_sizes:
                if level in growth[cf_name]:
                    start_value = growth[cf_name][level][0]
                else:
                    start_value = None
                growth[cf_name][level] = (start_value,
                                          end_cf_level_sizes[level])
    return growth


def calc_cf_table_creation_stats(cf_name, events_mngr):
    assert isinstance(events_mngr, EventsMngr)

    creation_events = \
        events_mngr.get_cf_events_by_type(cf_name,
                                          EventType.TABLE_FILE_CREATION)

    total_num_entries = 0
    total_keys_sizes = 0
    total_values_sizes = 0
    for event in creation_events:
        table_properties = event.event_details_dict["table_properties"]
        total_num_entries += table_properties["num_entries"]
        total_keys_sizes += table_properties["raw_key_size"]
        total_values_sizes += table_properties["raw_value_size"]

    num_tables_created = len(creation_events)
    avg_num_table_entries = 0
    avg_key_size = 0
    avg_value_size = 0

    if num_tables_created > 0:
        avg_num_table_entries = int(total_num_entries / num_tables_created)
        avg_key_size = int(total_keys_sizes / total_num_entries)
        avg_value_size = int(total_values_sizes / total_num_entries)

    return {"num_tables_created": num_tables_created,
            "total_num_entries": total_num_entries,
            "total_keys_sizes": total_keys_sizes,
            "total_values_sizes": total_values_sizes,
            "avg_num_table_entries": avg_num_table_entries,
            "avg_key_size": avg_key_size,
            "avg_value_size": avg_value_size}


@dataclass
class DeleteOpersStats:
    total_num_flushed_entries: int = None
    total_num_deletes: int = None
    total_percent_deletes: float = None
    unavailability_reason: str = None


def calc_cf_delete_opers_stats(cf_name, events_mngr):
    flush_started_events = \
        events_mngr.get_cf_events_by_type(cf_name, EventType.FLUSH_STARTED)

    if not flush_started_events:
        return DeleteOpersStats(
            unavailability_reason=utils.NO_FLUSHES_TEXT)

    stats = DeleteOpersStats(total_num_flushed_entries=0, total_num_deletes=0)

    for flush_event in flush_started_events:
        stats.total_num_flushed_entries += flush_event.get_num_entries()
        stats.total_num_deletes += flush_event.get_num_deletes()

    return stats


def calc_delete_opers_stats(cfs_names, events_mngr):
    assert cfs_names
    assert isinstance(events_mngr, EventsMngr)

    stats = DeleteOpersStats(total_num_flushed_entries=0, total_num_deletes=0)

    unavailability_reason = None
    has_any_data = False
    for cf_name in cfs_names:
        cf_stats = calc_cf_delete_opers_stats(cf_name, events_mngr)
        assert isinstance(cf_stats, DeleteOpersStats)

        if cf_stats.total_num_flushed_entries:
            has_any_data = True
            stats.total_num_flushed_entries += \
                cf_stats.total_num_flushed_entries

            assert cf_stats.total_num_deletes is not None
            stats.total_num_deletes += cf_stats.total_num_deletes
        else:
            # arbitrarily use the first reason for all
            assert cf_stats.unavailability_reason is not None
            unavailability_reason = cf_stats.unavailability_reason

    if not has_any_data:
        return DeleteOpersStats(unavailability_reason=unavailability_reason)

    if stats.total_num_flushed_entries > 0:
        stats.total_percent_deletes = \
            float(100 * stats.total_num_deletes /
                  stats.total_num_flushed_entries)

    return stats


@dataclass
class UserOpersStats:
    num_written: int = None
    num_read: int = None
    num_seek: int = None
    total_num_user_opers: int = None
    percent_written: float = None
    percent_read: float = None
    percent_seek: float = None
    unavailability_reason: str = None


def get_user_operations_stats(counters_mngr):
    if not counters_mngr.does_have_counters_values():
        return UserOpersStats(unavailability_reason=utils.NO_STATS_TEXT)

    stats = UserOpersStats()
    mngr = counters_mngr
    stats.num_written = \
        mngr.get_last_counter_value("rocksdb.number.keys.written")
    stats.num_read = mngr.get_last_counter_value("rocksdb.number.keys.read")
    stats.num_seek = mngr.get_last_counter_value("rocksdb.number.db.seek")
    stats.total_num_user_opers = \
        stats.num_written + stats.num_read + stats.num_seek

    if stats.total_num_user_opers > 0:
        stats.percent_written = float(100 * stats.num_written /
                                      stats.total_num_user_opers)
        stats.percent_read = float(100 * stats.num_read /
                                   stats.total_num_user_opers)
        stats.percent_seek = float(100 * stats.num_seek /
                                   stats.total_num_user_opers)

    return stats


@dataclass
class LogFileTimeInfo:
    start_time: str = None
    end_time: str = None
    span_seconds: float = 0.0


def get_log_file_time_info(parsed_log):
    metadata = parsed_log.get_metadata()

    return LogFileTimeInfo(start_time=metadata.get_start_time(),
                           end_time=metadata.get_end_time(),
                           span_seconds=metadata.get_log_time_span_seconds())


def get_warn_messages(raw_elements):
    if not raw_elements:
        return None

    returned_errors = dict()
    for cf_errors in raw_elements.values():
        for category_errors in cf_errors.values():
            for error_info in category_errors:
                assert isinstance(error_info, WarningElementInfo)
                returned_errors[error_info.time] = error_info.warning_msg

    return returned_errors


def get_error_warnings(warnings_mngr):
    assert isinstance(warnings_mngr, WarningsMngr)
    return get_warn_messages(warnings_mngr.get_error_warnings())


def get_fatal_warnings(warnings_mngr):
    assert isinstance(warnings_mngr, WarningsMngr)
    return get_warn_messages(warnings_mngr.get_fatal_warnings())


def get_db_wide_info(parsed_log: ParsedLog):
    metadata = parsed_log.get_metadata()
    warns_mngr = parsed_log.get_warnings_mngr()
    stats_mngr = parsed_log.get_stats_mngr()
    db_wide_stats_mngr = stats_mngr.get_db_wide_stats_mngr()
    counters_mngr = parsed_log.get_counters_mngr()

    user_opers_stats = get_user_operations_stats(counters_mngr)
    assert isinstance(user_opers_stats, UserOpersStats)

    cumulative_writes_stats_dict = \
        stats_mngr.get_db_wide_stats_mngr(). \
        get_last_cumulative_writes_entry()

    num_keys_written = None
    if cumulative_writes_stats_dict:
        _, cumulative_writes_stats = \
            utils.get_first_dict_entry_components(cumulative_writes_stats_dict)
        if user_opers_stats.num_written:
            num_keys_written = max(user_opers_stats.num_written,
                                   cumulative_writes_stats.num_keys)
        else:
            num_keys_written = cumulative_writes_stats.num_keys

    total_num_table_created_entries = 0
    total_keys_sizes = 0
    total_values_size = 0

    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    events_mngr = parsed_log.get_events_mngr()

    delete_opers_stats = calc_delete_opers_stats(cfs_names, events_mngr)
    assert isinstance(delete_opers_stats, DeleteOpersStats)

    for cf_name in cfs_names:
        table_creation_stats = calc_cf_table_creation_stats(cf_name,
                                                            events_mngr)
        total_num_table_created_entries += \
            table_creation_stats["total_num_entries"]
        total_keys_sizes += table_creation_stats["total_keys_sizes"]
        total_values_size += table_creation_stats["total_values_sizes"]

    # TODO - Add unit test when total_num_table_created_entries == 0
    # TODO - Consider whether this means data is not available
    avg_key_size_bytes = None
    avg_value_size_bytes = None
    if total_num_table_created_entries > 0:
        avg_key_size_bytes = \
            int(total_keys_sizes / total_num_table_created_entries)
        avg_value_size_bytes = \
            int(total_values_size / total_num_table_created_entries)

    compactions_stats_mngr = \
        parsed_log.get_stats_mngr().get_compactions_stats_mngr()

    db_size_bytes_info = get_db_size_bytes_info_at_end(cfs_names,
                                                       compactions_stats_mngr)
    assert isinstance(db_size_bytes_info, DbSizeBytesInfo)

    ingest_info = get_db_ingest_info(db_wide_stats_mngr)

    info = {
        "creator": metadata.get_product_name(),
        "version": metadata.get_version(),
        "git_hash": metadata.get_git_hash(),
        "db_size_bytes": db_size_bytes_info.size_bytes,
        "db_size_bytes_time": db_size_bytes_info.size_time,
        "num_cfs": parsed_log.get_num_cfs_when_certain(),
        "avg_key_size_bytes": avg_key_size_bytes,
        "avg_value_size_bytes": avg_value_size_bytes,
        "num_warnings": warns_mngr.get_total_num_warns(),
        "errors": get_error_warnings(warns_mngr),
        "fatals": get_fatal_warnings(warns_mngr),
        "total_num_table_created_entries": total_num_table_created_entries,
        "num_keys_written": num_keys_written,
        "user_opers_stats": user_opers_stats,
        "delete_opers_stats": delete_opers_stats,
        "ingest_info": ingest_info
    }

    return info


@dataclass
class DbIngestInfo:
    time: str
    ingest: int = 0
    ingest_rate_mbps: float = 0.0


def get_db_ingest_info(db_wide_stats_mngr):
    assert isinstance(db_wide_stats_mngr, DbWideStatsMngr)

    cumulative_writes_stats_dict = \
        db_wide_stats_mngr.get_last_cumulative_writes_entry()
    if not cumulative_writes_stats_dict:
        return None

    time, cumulative_writes_stats = \
        utils.get_first_dict_entry_components(cumulative_writes_stats_dict)

    return DbIngestInfo(
        time=time,
        ingest=cumulative_writes_stats.ingest,
        ingest_rate_mbps=cumulative_writes_stats.ingest_rate_mbps)


def calc_event_histogram(cf_name, events_mngr, event_type, group_by_field):
    events = events_mngr.get_cf_events_by_type(cf_name, event_type)

    histogram = dict()
    for event in events:
        event_grouping = event.event_details_dict[group_by_field]
        if event_grouping not in histogram:
            histogram[event_grouping] = 0
        histogram[event_grouping] += 1

    return histogram


# A list of flushed data sizes (in MB) to use for the generation of the
# flush sizes histogram
# The sizes are integers and must be ordered and always increasing
FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB = [2, 10, 32, 64]


@dataclass
class PerFlushReasonStats:
    num_flushes: int = 0

    # Prepare a count per bucket (+1 for sizes > last)
    sizes_histogram: list = \
        field(default_factory=lambda: ([0] * (len(
            FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB)+1)))

    min_duration_ms: int = None
    max_duration_ms: int = None
    min_num_memtables: int = None
    max_num_memtables: int = None
    min_total_data_size_bytes: int = None
    max_total_data_size_bytes: int = None


def calc_cf_flushes_stats(cf_name, events_mngr):
    cf_flush_events = events_mngr.get_cf_flow_events(FlowType.FLUSH,
                                                     cf_name)
    if not cf_flush_events:
        return {}

    def get_min(curr, new):
        return min(curr, new) if curr is not None else new

    def get_max(curr, new):
        return max(curr, new) if curr is not None else new

    stats = {}
    for events_pair in cf_flush_events:
        start_flush_event = events_pair[0]
        flush_reason = start_flush_event.get_flush_reason()

        num_memtables = start_flush_event.get_num_memtables()
        total_data_size_bytes = start_flush_event.get_total_data_size_bytes()

        flush_duration_ms = 0
        # It's possible that there is no matching end event
        end_event = events_pair[1]
        if end_event:
            start_event_info = MatchingEventInfo(start_flush_event,
                                                 FlowType.FLUSH, True)
            end_event_info = MatchingEventInfo(end_event,
                                               FlowType.FLUSH, False)
            flush_duration_ms = \
                start_event_info.get_duration_ms(end_event_info)

        # Find the bucket for the flushed size
        bucket_idx = bisect(FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB,
                            total_data_size_bytes / (2**20))

        if flush_reason not in stats:
            stats[flush_reason] = PerFlushReasonStats()

        reason_stats = stats[flush_reason]
        reason_stats.num_flushes += 1
        reason_stats.sizes_histogram[bucket_idx] += 1
        reason_stats.min_duration_ms = \
            get_min(reason_stats.min_duration_ms, flush_duration_ms)
        reason_stats.max_duration_ms = \
            get_max(reason_stats.max_duration_ms, flush_duration_ms)
        reason_stats.min_num_memtables = \
            get_min(reason_stats.min_num_memtables, num_memtables)
        reason_stats.max_num_memtables = \
            get_max(reason_stats.max_num_memtables, num_memtables)
        reason_stats.min_total_data_size_bytes = \
            get_min(reason_stats.min_total_data_size_bytes,
                    total_data_size_bytes)
        reason_stats.max_total_data_size_bytes = \
            get_max(reason_stats.max_total_data_size_bytes,
                    total_data_size_bytes)

    if stats:
        for reason in stats:
            stats[reason] = stats[reason]

    return stats


def get_largest_compaction_size_bytes(compactions_monitor):
    jobs = compactions_monitor.get_finished_jobs()

    largest_size_bytes = 0
    for job in jobs.values():
        largest_size_bytes = max(largest_size_bytes,
                                 job.start_event.get_input_data_size_bytes())

    return largest_size_bytes


def get_cf_per_level_write_amp(compactions_stats_mngr, cf_name):
    cf_compaction_stats = \
        compactions_stats_mngr.get_cf_level_entries(cf_name)
    if not cf_compaction_stats:
        return None


@dataclass
class CfCompactionStats:
    num_compactions: int = 0
    min_compaction_bw_mbps: float = None
    max_compaction_bw_mbps: float = None
    per_level_write_amp: dict = None
    comp_sec: float = None
    comp_merge_cpu_sec: float = None


def calc_cf_compactions_stats(cf_name, log_start_time, compactions_monitor,
                              compactions_stats_mngr):
    cf_jobs = compactions_monitor.get_cf_finished_jobs(cf_name)
    if not cf_jobs:
        return None

    stats = CfCompactionStats()
    for job in cf_jobs.values():
        stats.num_compactions += 1

        if job.pre_finish_info:
            if stats.min_compaction_bw_mbps is None:
                stats.min_compaction_bw_mbps = \
                    job.pre_finish_info.write_rate_mbps
            else:
                stats.min_compaction_bw_mbps = \
                    min(stats.min_compaction_bw_mbps,
                        job.pre_finish_info.write_rate_mbps)
            if stats.max_compaction_bw_mbps is None:
                stats.max_compaction_bw_mbps = \
                    job.pre_finish_info.write_rate_mbps
            else:
                stats.max_compaction_bw_mbps = \
                    max(stats.max_compaction_bw_mbps,
                        job.pre_finish_info.write_rate_mbps)

    last_entry = compactions_stats_mngr.get_last_cf_level_entry(cf_name)
    if last_entry:
        per_level_write_amp = \
            CompactionStatsMngr.get_field_value_for_all_levels(
                last_entry, CompactionStatsMngr.LevelFields.WRITE_AMP)
        if per_level_write_amp:
            sum_write_amp = \
                CompactionStatsMngr.get_sum_value(
                    last_entry, CompactionStatsMngr.LevelFields.WRITE_AMP)
            per_level_write_amp["SUM"] = sum_write_amp
            stats.per_level_write_amp = per_level_write_amp

        uptime = \
            CompactionStatsMngr.get_level_entry_uptime_seconds(last_entry,
                                                               log_start_time)
        if uptime > 0.0:
            stats.comp_sec = \
                float(CompactionStatsMngr.get_sum_value(
                    last_entry, CompactionStatsMngr.LevelFields.COMP_SEC))
            stats.comp_merge_cpu_sec =\
                float(CompactionStatsMngr.get_sum_value(
                    last_entry,
                    CompactionStatsMngr.LevelFields.COMP_MERGE_CPU))

    return stats


def calc_all_events_histogram(cf_names, events_mngr):
    # Returns a dictionary of:
    # {<cf_name>: {<event_type>: [events]}}   # noqa
    histogram = {}

    for cf_name in cf_names:
        for event_type in EventType:
            cf_events_of_type = events_mngr.get_cf_events_by_type(cf_name,
                                                                  event_type)
            if cf_name not in histogram:
                histogram[cf_name] = {}

            if cf_events_of_type:
                histogram[cf_name][event_type] = len(cf_events_of_type)
    return histogram


def is_cf_compression_by_level(parsed_log, cf_name):
    db_opts = parsed_log.get_database_options()
    return db_opts.get_cf_option(cf_name, "compression[0]") is not None


def get_applicable_cf_options(db_opts):
    assert isinstance(db_opts, db_options.DatabaseOptions)

    cf_names = db_opts.get_cfs_names()
    cfs_options = {"compaction_style": {},
                   "compression": {},
                   "filter_policy": {}}

    for cf_name in cf_names:
        cfs_options["compaction_style"][cf_name] = \
            db_opts.get_cf_option(cf_name, "compaction_style")
        cfs_options["compression"][cf_name] = \
            db_opts.get_cf_option(cf_name, "compression")
        cfs_options["filter_policy"][cf_name] = \
            db_opts.get_cf_table_option(cf_name, "filter_policy")

    compaction_styles = list(set(cfs_options["compaction_style"].values()))
    if len(compaction_styles) == 1 and compaction_styles[0] is not None:
        common_compaction_style = compaction_styles[0]
    else:
        common_compaction_style = "Per Column Family"
    cfs_options["compaction_style"]["common"] = common_compaction_style

    compressions = list(set(cfs_options["compression"].values()))
    if len(compressions) == 1 and compressions[0] is not None:
        common_compression = compressions[0]
    else:
        common_compression = "Per Column Family"
    cfs_options["compression"]["common"] = common_compression

    filter_policies = list(set(cfs_options["filter_policy"].values()))
    if len(filter_policies) == 1 and filter_policies[0] is not None:
        common_filter_policy = filter_policies[0]
    else:
        common_filter_policy = "Per Column Family"
    cfs_options["filter_policy"]["common"] = common_filter_policy

    return cfs_options


@dataclass
class CfReadLatencyStats:
    num_reads: int = 0
    avg_read_latency_us: float = 0.0
    max_read_latency_us: float = 0.0
    read_percent_of_all_cfs: float = 0.0


def calc_read_latency_per_cf_stats(cf_file_histogram_stats_mngr):
    stats = {}

    all_entries = cf_file_histogram_stats_mngr.get_all_entries()
    if not all_entries:
        return {}

    total_num_reads = 0
    for cf_name in all_entries:
        last_cf_entry = cf_file_histogram_stats_mngr.get_last_cf_entry(cf_name)
        if not last_cf_entry:
            continue
        total_cf_num_reads = 0
        total_cf_read_latency_us = 0.0
        max_cf_read_latency_us = 0.0

        levels_stats = CfFileHistogramStatsMngr.CfEntry(last_cf_entry)
        for level_stats in levels_stats.get_all_levels_stats().values():
            total_cf_num_reads += level_stats.count
            total_cf_read_latency_us += \
                level_stats.count * level_stats.average
            max_cf_read_latency_us = \
                max(max_cf_read_latency_us, level_stats.max)

        avg_cf_read_latency_us = total_cf_read_latency_us / total_cf_num_reads
        stats[cf_name] = \
            CfReadLatencyStats(
                num_reads=total_cf_num_reads,
                avg_read_latency_us=avg_cf_read_latency_us,
                max_read_latency_us=max_cf_read_latency_us)
        total_num_reads += total_cf_num_reads

    for cf_stats in stats.values():
        cf_stats.read_percent_of_all_cfs = \
            (cf_stats.num_reads/total_num_reads) * 100
    return stats if stats else None


def calc_cf_read_density(compactions_stats_mngr, cf_file_histogram_stats_mngr,
                         cf_read_latecny_stats, cf_name):
    assert isinstance(cf_read_latecny_stats, CfReadLatencyStats)

    last_cf_compaction_stats_entry = \
        compactions_stats_mngr.get_last_cf_level_entry(cf_name)
    if not last_cf_compaction_stats_entry:
        return None

    last_cf_read_latency_raw_entry = \
        cf_file_histogram_stats_mngr.get_last_cf_entry(cf_name)
    last_cf_read_latency_entry = \
        CfFileHistogramStatsMngr.CfEntry(last_cf_read_latency_raw_entry)

    total_num_cf_reads = cf_read_latecny_stats.num_reads
    if total_num_cf_reads == 0:
        logging.info(f"No read density as no reads for cf ({cf_name}). "
                     f"time:{last_cf_read_latency_entry.get_time()} ")
        return {}

    # Calculate per level READ-NORM
    per_level_read_norm = dict()
    for level, level_stats in \
            last_cf_read_latency_entry.get_all_levels_stats().items():
        per_level_read_norm[level] = level_stats.count / total_num_cf_reads

    compaction_entry = CompactionStatsMngr.CfLevelEntry(
        last_cf_compaction_stats_entry)

    total_cf_size_bytes = \
        compactions_stats_mngr.get_cf_size_bytes_at_end(cf_name)
    if total_cf_size_bytes == 0:
        logging.info(f"No read density as cf ({cf_name}) size if 0 "
                     f"time:{compaction_entry.get_time()} ")
        return None

    # Calculate per level SIZE-NORM
    per_level_size_norm = dict()
    compaction_levels = compaction_entry.get_levels()
    for level in compaction_levels:
        # TODO - If this ever gets used again - level_size_bytes could be None
        level_size_bytes = \
            compactions_stats_mngr.get_cf_level_size_bytes(cf_name, level)
        per_level_size_norm[level] = level_size_bytes / total_cf_size_bytes

    per_level_read_density = dict()
    for level, level_size_bytes in per_level_size_norm.items():
        if level_size_bytes == 0:
            logging.info(f"0 Size level {level} skipped."
                         f"cf:{cf_name}. time:{compaction_entry.get_time()}")
            continue
        if level not in per_level_read_norm:
            logging.info(f"Level {level} skipped since it's missing in "
                         f"compaction levels dump. cf:{cf_name}. time:"
                         f"{compaction_entry.get_time()}")
            continue

        per_level_read_density[level] =\
            per_level_read_norm[level] / per_level_size_norm[level]

    # Calculate the weighted-average of the norms
    sum_densities = sum(per_level_read_density.values())
    per_level_weighted_avg_density = \
        {level: density / sum_densities for level, density in
         per_level_read_density.items()}

    return per_level_weighted_avg_density


@dataclass
class SeekStats:
    num_seeks: int = 0
    num_found_seeks: int = 0
    num_nexts: int = 0
    num_prevs: int = 0
    avg_seek_range_size: float = 0.0
    avg_seek_rate_per_second: float = 0.0
    avg_seek_latency_us: float = 0.0


def get_applicable_seek_stats(counters_mngr):
    # The names of the counters in the stats dump in the log
    prefix = 'rocksdb.number.db'
    seek_name = f"{prefix}.seek"
    seek_found_name = f"{prefix}.seek.found"
    seek_next_name = f"{prefix}.next"
    seek_prev_name = f"{prefix}.prev"
    seek_latency_hist_us = "rocksdb.db.seek.micros"

    mngr = counters_mngr

    # First see if there are any seeks recorded
    last_seek_entry = mngr.get_last_counter_entry(seek_name)
    if not last_seek_entry:
        logging.info("No seeks (maybe no stats) or no actual seeks in log.")
        return None

    last_seek_time = last_seek_entry["time"]
    last_seek_count = last_seek_entry["value"]

    first_seek_entry = mngr.get_first_counter_entry(seek_name)
    first_seek_time = first_seek_entry["time"]
    first_seek_count = first_seek_entry["value"]

    num_seeks = last_seek_count - first_seek_count
    if num_seeks == 0:
        logging.info("No seeks log (seek counter is 0).")
        return None

    seek_time_span_seconds = \
        utils.get_times_strs_diff_seconds(
            first_seek_time, last_seek_time)

    stats = SeekStats()

    last_seek_found_count = mngr.get_last_counter_value(seek_found_name)
    last_num_nexts = mngr.get_last_counter_value(seek_next_name)
    last_num_prevs = mngr.get_last_counter_value(seek_prev_name)

    first_seek_found_count = mngr.get_first_counter_value(seek_found_name)
    first_num_nexts = mngr.get_first_counter_value(seek_next_name)
    first_num_prevs = mngr.get_first_counter_value(seek_prev_name)

    stats.num_seeks = num_seeks
    stats.num_found_seeks = last_seek_found_count - first_seek_found_count
    stats.num_nexts = last_num_nexts - first_num_nexts
    stats.num_prevs = last_num_prevs - first_num_prevs
    if stats.num_seeks > 0:
        stats.avg_seek_range_size = \
            (stats.num_prevs + stats.num_nexts) / stats.num_seeks

    avg_seek_latency_us = \
        mngr.get_last_histogram_entry(seek_latency_hist_us, non_zero=True)

    if avg_seek_latency_us:
        if seek_time_span_seconds > 0.0:
            stats.avg_seek_rate_per_second = \
                num_seeks / seek_time_span_seconds
        seek_latency_hist = avg_seek_latency_us["values"]
        stats.avg_seek_latency_us = seek_latency_hist["Average"]

    return stats


def get_warn_warnings_info(cfs_names, warnings_mngr):
    assert isinstance(warnings_mngr, WarningsMngr)

    all_warn_warnings = warnings_mngr.get_warnings_of_type(WarningType.WARN)
    if not all_warn_warnings:
        return None

    returned_warn_warnings = {}
    cfs_names_including_db = [utils.NO_CF] + cfs_names
    for cf_name in cfs_names_including_db:
        if cf_name in all_warn_warnings:
            cf_info = copy.deepcopy(all_warn_warnings[cf_name])
            for category in list(cf_info.keys()):
                cf_info[category] = len(cf_info[category])
            returned_warn_warnings[cf_name] = cf_info
        else:
            returned_warn_warnings[cf_name] = {}
    return returned_warn_warnings


@dataclass
class CfFilterFilesStats:
    filter_policy: str = None
    avg_bpk: float = None


def calc_files_filter_stats(cfs_names, db_opts, files_monitor):
    assert isinstance(db_opts, db_options.DatabaseOptions)
    assert isinstance(files_monitor, db_files.DbFilesMonitor)

    stats = dict()

    cfs_options = get_applicable_cf_options(db_opts)
    cfs_filters_from_options = dict()
    for cf_name in cfs_names:
        if cf_name in cfs_options['filter_policy']:
            cfs_filters_from_options[cf_name] = \
                cfs_options['filter_policy'][cf_name]

    for cf_name in cfs_names:
        cf_filter_files_stats = \
            db_files.calc_cf_files_stats([cf_name], files_monitor)
        if cf_filter_files_stats:
            assert isinstance(cf_filter_files_stats, db_files.CfsFilesStats)

            filter_policy = \
                cf_filter_files_stats.cfs_filter_specific[
                    cf_name].filter_policy
            avg_bpk = \
                cf_filter_files_stats.cfs_filter_specific[cf_name].avg_bpk

            stats[cf_name] = CfFilterFilesStats(filter_policy=filter_policy,
                                                avg_bpk=avg_bpk)
        elif cf_name in cfs_filters_from_options:
            filter_policy = cfs_filters_from_options[cf_name]
            stats[cf_name] = CfFilterFilesStats(filter_policy=filter_policy,
                                                avg_bpk=None)
        else:
            # INVALID_FILTER_POLICY to indicate this cf's filter policy
            # can't be deduced (instead of None which means - we know it has
            # no filter policy)
            stats[cf_name] = CfFilterFilesStats(
                filter_policy=utils.INVALID_FILTER_POLICY, avg_bpk=None)

    return stats


@dataclass
class FilterCounters:
    negatives: int = 0
    positives: int = 0
    true_positives: int = 0
    false_positives: int = 0
    one_in_n_fpr: int = 0

    def are_all_zeroes(self):
        return self.negatives + self.positives + self.true_positives + \
               self.false_positives + self.one_in_n_fpr == 0


def collect_filter_counters(counters_mngr):
    assert isinstance(counters_mngr, CountersMngr)

    if not counters_mngr.does_have_counters_values():
        logging.info("Can't collect Filter counters. No counters available")
        return None

    filter_counters_names = {
        "negatives": "rocksdb.bloom.filter.useful",
        "positives":  "rocksdb.bloom.filter.full.positive",
        "true_positives": "rocksdb.bloom.filter.full.true.positive"}

    counters = FilterCounters()

    for field_name, counter_name in filter_counters_names.items():
        counter_value = counters_mngr.get_last_counter_value(counter_name)
        setattr(counters, field_name, counter_value)

    assert counters.positives >= counters.true_positives
    counters.false_positives = counters.positives - counters.true_positives

    return counters


def calc_bloom_1_in_fpr(counters):
    assert isinstance(counters, FilterCounters)
    total = counters.negatives + counters.positives
    false_positives = counters.false_positives
    if false_positives == 0:
        return 0
    return int(total / false_positives)


@dataclass
class FilterStats:
    files_filter_stats: dict = None
    filter_counters: FilterCounters = None


def calc_filter_stats(cfs_names, db_opts, files_monitor, counters_mngr):
    assert isinstance(db_opts, db_options.DatabaseOptions)
    assert isinstance(files_monitor, db_files.DbFilesMonitor)
    assert isinstance(counters_mngr, CountersMngr)

    stats = FilterStats()
    files_filter_stats = calc_files_filter_stats(cfs_names,
                                                 db_opts,
                                                 files_monitor)
    if files_filter_stats:
        stats.files_filter_stats = files_filter_stats

    filter_counters = collect_filter_counters(counters_mngr)
    if filter_counters:
        assert isinstance(filter_counters, FilterCounters)

        one_in_n_fpr = calc_bloom_1_in_fpr(filter_counters)
        filter_counters.one_in_n_fpr = one_in_n_fpr
        stats.filter_counters = filter_counters

    return stats


def get_cfs_common_and_specific_options(db_opts):
    assert isinstance(db_opts, db_options.DatabaseOptions)

    cfs_names = db_opts.get_cfs_names()
    cfs_options = {cf_name: db_opts.get_cf_options(cf_name)
                   for cf_name in cfs_names}

    cfs_common_options, cfs_specific_options = \
        db_opts.get_unified_cfs_options(cfs_options)

    return cfs_common_options, cfs_specific_options


def get_cfs_common_and_specific_diff_dicts(
        baseline_options, log_database_options):
    assert isinstance(baseline_options, db_options.DatabaseOptions)
    assert isinstance(log_database_options, db_options.DatabaseOptions)

    baseline_opts = baseline_options.get_all_options()

    cfs_common_options, cfs_specific_options =  \
        get_cfs_common_and_specific_options(log_database_options)

    common_dummy_cf_name = "COMMON-DUMMY-CF-NAME"
    common_log_file_full_name_options = db_options.FullNamesOptionsDict()
    common_log_file_full_name_options.\
        init_from_full_names_options_no_cf_dict(common_dummy_cf_name,
                                                cfs_common_options)

    # We need to compare the common to the baseline,
    # but a baseline that only contains the options common
    # to all the cf-s
    baseline_opts_for_diff_dict = dict()
    baseline_opts_dict = baseline_opts.get_options_dict()
    for full_common_option_name in cfs_common_options.keys():
        if full_common_option_name in baseline_opts_dict:
            baseline_opts_for_diff_dict[full_common_option_name] = \
                baseline_opts_dict[full_common_option_name]
    baseline_opts_for_diff =\
        db_options.FullNamesOptionsDict(baseline_opts_for_diff_dict)

    common_diff = db_options.DatabaseOptions.get_cfs_options_diff(
        baseline_opts_for_diff,
        utils.DEFAULT_CF_NAME,
        common_log_file_full_name_options,
        common_dummy_cf_name)
    if common_diff is None or common_diff.is_empty_diff():
        common_diff = {}

    # We need to compare every cf to the baseline, but a baseline that doesn't
    # have the options common to all the cf-s (they are missing in the
    # cfs_specific_options)
    baseline_opts_for_diff_dict = \
        copy.deepcopy(baseline_opts.get_options_dict())
    utils.delete_dict_keys(baseline_opts_for_diff_dict,
                           cfs_common_options.keys())
    baseline_opts_for_diff =\
        db_options.FullNamesOptionsDict(baseline_opts_for_diff_dict)

    cfs_specific_diffs = dict()
    for cf_name, cf_options in cfs_specific_options.items():
        cf_full_name_options = db_options.FullNamesOptionsDict()
        cf_full_name_options. \
            init_from_full_names_options_no_cf_dict(cf_name, cf_options)

        cf_diff = db_options.DatabaseOptions.get_cfs_options_diff(
            baseline_opts_for_diff,
            utils.DEFAULT_CF_NAME,
            cf_full_name_options,
            cf_name)
        cfs_specific_diffs[cf_name] = cf_diff

    return common_diff, cfs_specific_diffs
