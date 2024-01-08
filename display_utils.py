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

import io
import logging
from dataclasses import dataclass, asdict
from pathlib import Path

import baseline_log_files_utils
import cache_utils
import calc_utils
import db_files
import db_options
import log_file
import utils
from counters import CountersMngr
from db_options import DatabaseOptions, CfsOptionsDiff, SectionType
from db_options import SanitizedValueType
from stats_mngr import CompactionStatsMngr, StatsMngr
from warnings_mngr import WarningType, WarningElementInfo, WarningsMngr

num_for_display = utils.get_human_readable_number
num_bytes_for_display = utils.get_human_readable_num_bytes

CFS_COMMON_KEY = "CF-s (Common)"
CFS_SPECIFIC_KEY = "CF-s (Specific)"
TABLE_KEY = "Block-Based Table"


def format_value(value, suffix=None, conv_func=None):
    if value is not None:
        if conv_func is not None:
            value = conv_func(value)
        if suffix is not None:
            suffix = " " + suffix
        else:
            suffix = ""
        return f"{value}{suffix}"
    else:
        return "No Information"


def prepare_db_wide_user_opers_stats_for_display(db_wide_info):
    display_info = dict()

    def get_disp_value(percent, num, total_num, oper_name,
                       unavailability_reason):
        if unavailability_reason is None:
            assert total_num is not None
            if total_num > 0 and num > 0:
                return f"{percent:.1f}% ({num}/{total_num})"
            else:
                return f"0 (No {oper_name} Operations)"
        else:
            return f"{utils.DATA_UNAVAILABLE_TEXT} ({unavailability_reason})"

    user_opers_stats = db_wide_info["user_opers_stats"]
    assert isinstance(user_opers_stats, calc_utils.UserOpersStats)

    total_num_user_opers = user_opers_stats.total_num_user_opers
    reason = user_opers_stats.unavailability_reason
    display_info['Writes'] = \
        get_disp_value(user_opers_stats.percent_written,
                       user_opers_stats.num_written,
                       total_num_user_opers,
                       "Write", reason)
    display_info['Reads'] = \
        get_disp_value(user_opers_stats.percent_read,
                       user_opers_stats.num_read,
                       total_num_user_opers,
                       "Read", reason)
    display_info['Seeks'] = \
        get_disp_value(user_opers_stats.percent_seek,
                       user_opers_stats.num_seek,
                       total_num_user_opers,
                       "Seek", reason)

    delete_opers_stats = db_wide_info["delete_opers_stats"]
    assert isinstance(delete_opers_stats, calc_utils.DeleteOpersStats)

    display_info['Deleted (Flushed) Entries'] = \
        get_disp_value(delete_opers_stats.total_percent_deletes,
                       delete_opers_stats.total_num_deletes,
                       delete_opers_stats.total_num_flushed_entries,
                       "Delete",
                       delete_opers_stats.unavailability_reason)

    return display_info


@dataclass
class NotableEntityInfo:
    display_title: str
    display_text: str
    special_value_type: SanitizedValueType
    special_value_text: str
    display_value: bool


notable_entities = {
    "statistics": NotableEntityInfo("Statistics",
                                    "Available",
                                    SanitizedValueType.NULL_PTR,
                                    utils.NO_STATS_TEXT,
                                    display_value=False)
}


def get_db_wide_notable_entities_display_info(parsed_log):
    display_info = {}
    db_opts = parsed_log.get_database_options()
    for option_name, info in notable_entities.items():
        option_value = db_opts.get_db_wide_option(option_name)
        if option_value is None:
            logging.warning(f"Option {option_name} not found in "
                            f"{parsed_log.get_log_file_path()}")
            continue

        option_value_type = SanitizedValueType.get_type_from_str(option_value)
        if option_value_type == info.special_value_type:
            display_value = info.special_value_text
        else:
            if info.display_text is None:
                display_value = option_value
            else:
                if info.display_value:
                    display_value = f"{info.display_text} ({option_value})"
                else:
                    display_value = info.display_text
        display_info[info.display_title] = display_value

    return display_info


def prepare_error_or_fatal_warnings_for_display(warnings_mngr, prepare_error):
    assert isinstance(warnings_mngr, WarningsMngr)

    if prepare_error:
        all_type_warnings = \
            warnings_mngr.get_warnings_of_type(WarningType.ERROR)
        if not all_type_warnings:
            return "No Errors"
    else:
        all_type_warnings = \
            warnings_mngr.get_warnings_of_type(WarningType.FATAL)
        if not all_type_warnings:
            return "No Fatals"

    warnings_tuples = list()
    for cf_name, cf_info in all_type_warnings.items():
        for category, err_infos in cf_info.items():
            for info in err_infos:
                assert isinstance(info, WarningElementInfo)
                warnings_tuples.append((info.time, info.warning_msg))

    # First item in every tuple is time => will be sorted on time
    warnings_tuples.sort()

    return {time: msg for time, msg in warnings_tuples}


def prepare_ingest_info_for_db_wide_info_display(db_wide_info):
    ingest_info = db_wide_info["ingest_info"]

    if ingest_info is not None:
        assert isinstance(ingest_info, calc_utils.DbIngestInfo)
        return prepare_db_ingest_info_for_display(ingest_info)
    else:
        unavailability_reason = utils.NO_INGEST_TEXT
        return {
            "Ingest": unavailability_reason,
            "Ingest Rate": unavailability_reason,
            "Ingest Time": None
        }


def prepare_db_wide_info_for_display(parsed_log):
    log_file_time_info = calc_utils.get_log_file_time_info(parsed_log)
    assert isinstance(log_file_time_info, calc_utils.LogFileTimeInfo)

    display_info = {}

    db_wide_info = calc_utils.get_db_wide_info(parsed_log)
    db_wide_notable_entities = \
        get_db_wide_notable_entities_display_info(parsed_log)
    display_info["Name"] = str(Path(parsed_log.get_log_file_path()))
    display_info["Start Time"] = log_file_time_info.start_time
    display_info["End Time"] = log_file_time_info.end_time
    display_info["Log Time Span"] = \
        utils.convert_seconds_to_dd_hh_mm_ss(log_file_time_info.span_seconds)
    display_info["Creator"] = db_wide_info['creator']
    display_info["Version"] = f"{db_wide_info['version']} " \
                              f"[{db_wide_info['git_hash']}]"

    db_size_bytes_time = db_wide_info['db_size_bytes_time']

    if db_wide_info['db_size_bytes'] is not None:
        display_info["DB Size"] = \
            num_bytes_for_display(db_wide_info['db_size_bytes'])
    else:
        display_info["DB Size"] = utils.DATA_UNAVAILABLE_TEXT

    display_info["DB Size Time"] = db_size_bytes_time

    if db_wide_info["num_keys_written"] is not None:
        display_info["Num Keys Written"] = \
            num_for_display(db_wide_info['num_keys_written'])
    else:
        display_info["Num Keys Written"] = utils.DATA_UNAVAILABLE_TEXT

    if db_wide_info['avg_key_size_bytes'] is not None:
        display_info["Avg. Written Key Size"] = \
            num_bytes_for_display(db_wide_info['avg_key_size_bytes'])
    else:
        display_info["Avg. Written Key Size"] = utils.DATA_UNAVAILABLE_TEXT

    if db_wide_info['avg_value_size_bytes'] is not None:
        display_info["Avg. Written Value Size"] = \
            num_bytes_for_display(db_wide_info['avg_value_size_bytes'])
    else:
        display_info["Avg. Written Value Size"] = utils.DATA_UNAVAILABLE_TEXT

    display_info["Num Warnings"] = db_wide_info['num_warnings']

    if db_wide_info['errors'] is not None:
        display_info["Error Messages"] = db_wide_info['errors']
    else:
        display_info["Error Messages"] = "No Error Messages"

    if db_wide_info['fatals'] is not None:
        display_info["Fatal Messages"] = db_wide_info['fatals']
    else:
        display_info["Fatal Messages"] = "No Fatal Messages"

    display_info.update(
        prepare_ingest_info_for_db_wide_info_display(db_wide_info))

    display_info.update(db_wide_notable_entities)
    display_info.update(
        prepare_db_wide_user_opers_stats_for_display(db_wide_info))

    num_cfs_info_msg = \
        "Please see the 'Ability to determine the number of cf-s' section in the log parser's documentation for more information" # noqa
    if db_wide_info['num_cfs'] is not None:
        total_num_cfs = db_wide_info['num_cfs']
        num_non_auto_gen_cfs_with_options = \
            parsed_log.get_cfs_names_that_have_options(
                include_auto_generated=False)
        display_info['Num CF-s'] = total_num_cfs
        if total_num_cfs != len(num_non_auto_gen_cfs_with_options):
            display_info["Num CF-s Info"] = num_cfs_info_msg
    else:
        display_info['Num CF-s'] = "Can't be accurately determined"
        display_info["Num CF-s Info"] = num_cfs_info_msg

    return display_info


def prepare_general_cf_info_for_display(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)

    filter_stats = \
        calc_utils.calc_filter_stats(cfs_names,
                                     parsed_log.get_database_options(),
                                     parsed_log.get_files_monitor(),
                                     parsed_log.get_counters_mngr())
    display_info = {}

    events_mngr = parsed_log.get_events_mngr()
    compaction_stats_mngr = \
        parsed_log.get_stats_mngr().get_compactions_stats_mngr()

    for cf_name in cfs_names:
        table_creation_stats = \
            calc_utils.calc_cf_table_creation_stats(cf_name, events_mngr)
        cf_options = calc_utils.get_applicable_cf_options(
            parsed_log.get_database_options())
        cf_size_bytes = compaction_stats_mngr.get_cf_size_bytes_at_end(cf_name)

        display_info[cf_name] = {}
        cf_display_info = display_info[cf_name]

        if cf_size_bytes is not None:
            cf_display_info["CF Size"] = num_bytes_for_display(cf_size_bytes)
        else:
            cf_display_info["CF Size"] = utils.DATA_UNAVAILABLE_TEXT

        cf_display_info["Avg. Key Size"] = \
            num_bytes_for_display(table_creation_stats['avg_key_size'])
        cf_display_info["Avg. Value Size"] = \
            num_bytes_for_display(table_creation_stats['avg_value_size'])

        if cf_name in cf_options['compaction_style']:
            if cf_options['compaction_style'][cf_name] is not None:
                cf_display_info["Compaction Style"] = \
                    cf_options['compaction_style'][cf_name]
            else:
                cf_display_info["Compaction Style"] = utils.UNKNOWN_VALUE_TEXT
        else:
            cf_display_info["Compaction Style"] = utils.UNKNOWN_VALUE_TEXT

        if cf_name in cf_options['compression']:
            if cf_options['compression'][cf_name] is not None:
                cf_display_info["Compression"] = \
                    cf_options['compression'][cf_name]
            else:
                cf_display_info["Compression"] = utils.UNKNOWN_VALUE_TEXT
        elif calc_utils.is_cf_compression_by_level(parsed_log, cf_name):
            cf_display_info["Compression"] = "Per-Level"
        else:
            cf_display_info["Compression"] = utils.UNKNOWN_VALUE_TEXT

        if cf_name in filter_stats.files_filter_stats:
            cf_files_filter_stats = filter_stats.files_filter_stats[cf_name]
        else:
            cf_files_filter_stats = None
        cf_display_info["Filter-Policy"] = \
            prepare_cf_filter_stats_for_display(cf_files_filter_stats,
                                                format_as_dict=False)

    return display_info


def prepare_warn_warnings_for_display(warn_warnings_info):
    # The input is in the following format:
    # {<warning-type>: {<cf-name>: {<category>: <number of messages>}}

    disp = dict()
    disp_db = dict()
    disp_cfs = dict()
    for cf_name, cf_info in warn_warnings_info.items():
        if cf_name == utils.NO_CF:
            disp_dict = disp_db
        else:
            disp_cfs[cf_name] = dict()
            disp_dict = disp_cfs[cf_name]

        for category, num_in_category in cf_info.items():
            disp_category = category.value
            disp_dict[disp_category] = num_in_category

    if not disp_db and not disp_cfs:
        return None

    if disp_db:
        disp["DB"] = disp_db
    else:
        disp["DB"] = "No DB Warnings"

    if disp_cfs:
        disp["CF-s"] = disp_cfs
    else:
        disp["CF-s"] = "No CF-s Warnings"

    return disp


def prepare_cfs_common_options_for_display(cfs_common_options):
    if cfs_common_options:
        options, table_options = \
            DatabaseOptions.prepare_flat_full_names_cf_options_for_display(
                cfs_common_options, None)
        return {
            "CF": options,
            TABLE_KEY: table_options
        }
    else:
        return "No Common Options to All CF-s"


def prepare_cfs_specific_options_for_display(cfs_specific_options):
    disp = {}

    if cfs_specific_options:
        for cf_name, cf_options in cfs_specific_options.items():
            if cf_options:
                disp_cf_options, disp_cf_table_options =\
                    DatabaseOptions.\
                    prepare_flat_full_names_cf_options_for_display(
                        cf_options, None)
                disp[cf_name] = {}
                if disp_cf_options:
                    disp[cf_name]["CF"] = \
                        disp_cf_options
                else:
                    disp[cf_name]["CF"] = \
                        "No Specific Options"
                if disp_cf_table_options:
                    disp[cf_name][TABLE_KEY] = \
                        disp_cf_table_options
                else:
                    disp[cf_name][TABLE_KEY] = \
                        "No Specific Table Options"
                if not disp[cf_name]:
                    del(disp[cf_name])

    if not disp:
        disp = "No Specific CF-s Options"

    return disp


def get_all_options_for_display(parsed_log):
    all_options = {}

    db_opts = parsed_log.get_database_options()

    cfs_common_options, cfs_specific_options =  \
        calc_utils.get_cfs_common_and_specific_options(db_opts)

    db_disp_opts = db_opts.get_db_wide_options_for_display()

    cfs_disp_opts = dict()
    cfs_disp_opts[CFS_COMMON_KEY] = \
        prepare_cfs_common_options_for_display(cfs_common_options)
    cfs_disp_opts[CFS_SPECIFIC_KEY] = \
        prepare_cfs_specific_options_for_display(cfs_specific_options)

    all_options["DB"] = db_disp_opts
    all_options["CF-s"] = cfs_disp_opts

    return all_options


def get_diff_tuple_for_display(raw_diff_tuple):
    return {utils.DIFF_BASELINE_NAME: raw_diff_tuple[0],
            utils.DIFF_LOG_NAME: raw_diff_tuple[1]}


def prepare_db_wide_diff_dict_for_display(
        product_name, baseline_log_path, baseline_version, db_wide_diff):
    display_db_wide_diff = {
        "Baseline": f"{str(baseline_version)} ({product_name})",
        "Baseline Log": str(baseline_log_path)
    }

    if db_wide_diff is None:
        display_db_wide_diff["DB"] = "No Diff"
        return display_db_wide_diff

    del (db_wide_diff[CfsOptionsDiff.CF_NAMES_KEY])
    display_db_wide_diff["DB"] = {}

    for full_option_name in db_wide_diff:
        section_type = SectionType.extract_section_type(full_option_name)
        option_name = \
            db_options.extract_option_name(full_option_name)

        if section_type == SectionType.DB_WIDE:
            display_db_wide_diff["DB"][option_name] = \
                get_diff_tuple_for_display(db_wide_diff[full_option_name])
        elif section_type == SectionType.VERSION:
            pass
        else:
            assert False, "Unexpected section type"

    if not display_db_wide_diff["DB"]:
        del (display_db_wide_diff["DB"])
    if list(display_db_wide_diff.keys()) == ["Baseline"]:
        display_db_wide_diff = {}

    return display_db_wide_diff


def prepare_cfs_diff_dict_for_display(common_diff, cfs_specific_diffs):
    display_cfs_diff = dict()

    if common_diff:
        assert isinstance(common_diff, db_options.CfsOptionsDiff)
        common_diff_dict = common_diff.get_diff_dict()
        del(common_diff_dict[db_options.CfsOptionsDiff.CF_NAMES_KEY])
        options, table_options = \
            DatabaseOptions.prepare_flat_full_names_cf_options_for_display(
                common_diff_dict, get_diff_tuple_for_display)
        display_cfs_diff[CFS_COMMON_KEY] = {
            "CF": options,
            TABLE_KEY: table_options
        }
    else:
        display_cfs_diff[CFS_COMMON_KEY] = "No Common Diff"

    display_cfs_diff[CFS_SPECIFIC_KEY] = dict()
    if cfs_specific_diffs:
        for cf_name, cf_specific_diff in cfs_specific_diffs.items():
            if cf_specific_diff is not None:
                assert isinstance(cf_specific_diff, db_options.CfsOptionsDiff)

                cf_specific_diff_dict = cf_specific_diff.get_diff_dict()
                del (cf_specific_diff_dict[
                    db_options.CfsOptionsDiff.CF_NAMES_KEY])
                options, table_options = \
                    DatabaseOptions.\
                    prepare_flat_full_names_cf_options_for_display(
                        cf_specific_diff_dict, get_diff_tuple_for_display)
                display_cfs_diff[CFS_SPECIFIC_KEY][cf_name] = {
                    "CF": options,
                    TABLE_KEY: table_options
                }
    if not display_cfs_diff[CFS_SPECIFIC_KEY]:
        display_cfs_diff[CFS_SPECIFIC_KEY] = "No CF-s Specific Diff"

    return display_cfs_diff


def get_options_baseline_diff_for_display(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    log_metadata = parsed_log.get_metadata()
    log_database_options = parsed_log.get_database_options()
    baseline_info = parsed_log.get_baseline_info()

    if baseline_info is None:
        return "NO BASELINE FOUND"

    assert isinstance(baseline_info,
                      baseline_log_files_utils.BaselineDBOptionsInfo)

    baseline_opts = baseline_info.baseline_options.get_all_options()
    log_opts = log_database_options.get_all_options()

    db_wide_diff = \
        DatabaseOptions.get_db_wide_options_diff(baseline_opts, log_opts)
    if db_wide_diff is not None:
        db_wide_diff = db_wide_diff.get_diff_dict()
    display_diff = prepare_db_wide_diff_dict_for_display(
        log_metadata.get_product_name(), baseline_info.baseline_log_path,
        baseline_info.closest_version, db_wide_diff)

    common_diff, cfs_specific_diffs = \
        calc_utils.get_cfs_common_and_specific_diff_dicts(
            baseline_info.baseline_options, log_database_options)

    display_diff["CF-s"] = \
        prepare_cfs_diff_dict_for_display(common_diff, cfs_specific_diffs)

    return display_diff


def prepare_cf_flushes_stats_for_display(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    disp = {}

    def calc_sizes_histogram():
        sizes_histogram = {}
        bucket_min_size_mb = 0
        for i, num_in_bucket in \
                enumerate(reason_stats.sizes_histogram):
            if i < len(calc_utils.FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB):
                bucket_max_size_mb = \
                    calc_utils.FLUSHED_SIZES_HISTOGRAM_BUCKETS_MB[i]
                bucket_title = f"{bucket_min_size_mb} - " \
                               f"{bucket_max_size_mb} [MB]"
            else:
                bucket_title = f"> {bucket_min_size_mb} [MB]"
            bucket_min_size_mb = bucket_max_size_mb

            sizes_histogram[bucket_title] = num_in_bucket
        return sizes_histogram

    def get_write_amp_level1():
        cf_compaction_stats =\
            compactions_stats_mngr.get_cf_level_entries(cf_name)
        if not cf_compaction_stats:
            return None
        last_dump_stats = cf_compaction_stats[-1]

        return CompactionStatsMngr.get_level_field_value(
            last_dump_stats, level=1,
            field=CompactionStatsMngr.LevelFields.WRITE_AMP)

    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    events_mngr = parsed_log.get_events_mngr()
    stats_mngr = parsed_log.get_stats_mngr()
    compactions_stats_mngr = stats_mngr.get_compactions_stats_mngr()

    for cf_name in cfs_names:
        cf_disp = dict()

        cf_flushes_stats = \
            calc_utils.calc_cf_flushes_stats(cf_name, events_mngr)
        if not cf_flushes_stats:
            continue

        write_amp_level1 = get_write_amp_level1()
        if not write_amp_level1:
            write_amp_level1 = utils.DATA_UNAVAILABLE_TEXT
        cf_disp["L0->L1 Write-Amp"] = write_amp_level1

        for reason, reason_stats in cf_flushes_stats.items():
            assert isinstance(reason_stats, calc_utils.PerFlushReasonStats)

            cf_reason_disp = dict()
            cf_reason_disp["Sizes Histogram"] = calc_sizes_histogram()
            cf_reason_disp["Num Flushes"] = \
                num_for_display(reason_stats.num_flushes)

            cf_reason_disp["Min Duration"] = \
                format_value(reason_stats.min_duration_ms,
                             suffix="ms",
                             conv_func=None)

            cf_reason_disp["Max Duration"] = \
                format_value(reason_stats.max_duration_ms,
                             suffix="ms",
                             conv_func=None)

            cf_reason_disp["Min Num Memtables"] = \
                format_value(reason_stats.min_num_memtables,
                             suffix=None,
                             conv_func=None)

            cf_reason_disp["Max Num Memtables"] = \
                format_value(reason_stats.max_num_memtables,
                             suffix=None,
                             conv_func=None)

            cf_reason_disp["Min Total Data Size"] = \
                format_value(reason_stats.min_total_data_size_bytes,
                             suffix=None,
                             conv_func=num_bytes_for_display)

            cf_reason_disp["Max Total Data Size"] = \
                format_value(reason_stats.max_total_data_size_bytes,
                             suffix=None,
                             conv_func=num_bytes_for_display)
            cf_disp[reason] = cf_reason_disp
        disp[cf_name] = cf_disp

    return disp


def prepare_global_compactions_stats_for_display(parsed_log):
    disp = {}
    compactions_monitor = parsed_log.get_compactions_monitor()
    largest_compaction_size_bytes = \
        calc_utils.get_largest_compaction_size_bytes(compactions_monitor)
    disp["Largest compaction size"] = \
        num_bytes_for_display(largest_compaction_size_bytes)
    return disp


def prepare_cf_compactions_stats_for_display(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    disp = {}

    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    log_start_time = parsed_log.get_metadata().get_start_time()
    compactions_monitor = parsed_log.get_compactions_monitor()
    compactions_stats_mngr = \
        parsed_log.get_stats_mngr().get_compactions_stats_mngr()

    for cf_name in cfs_names:
        cf_compactions_stats = \
            calc_utils.calc_cf_compactions_stats(
                cf_name, log_start_time, compactions_monitor,
                compactions_stats_mngr)

        if cf_compactions_stats:
            assert isinstance(cf_compactions_stats,
                              calc_utils.CfCompactionStats)
            s = cf_compactions_stats
            if s.per_level_write_amp is not None:
                per_level_write_amp = s.per_level_write_amp
            else:
                per_level_write_amp = "No Write-Amp Info Found"

            disp[cf_name] = {
                "Num Compactions": s.num_compactions,
                "Min Compactions BW":
                    format_value(s.min_compaction_bw_mbps, "MBPS"),
                "Max Compactions BW":
                    format_value(s.max_compaction_bw_mbps, "MBPS"),
                "Comp": format_value(s.comp_sec, "seconds"),
                "Comp Merge CPU": format_value(s.comp_merge_cpu_sec,
                                               "seconds"),
                "Per-Level Write-Amp": per_level_write_amp
            }

    return disp


def prepare_cf_stalls_entries_for_display(parsed_log):
    mngr = parsed_log.get_stats_mngr().get_cf_no_file_stats_mngr()
    stall_counts = mngr.get_stall_counts()

    display_stall_counts = {}
    for cf_name in stall_counts.keys():
        if stall_counts[cf_name]:
            display_stall_counts[cf_name] = stall_counts[cf_name]

    return display_stall_counts if display_stall_counts \
        else "No Stalls"


def generate_ascii_table(columns_names, table):
    f = io.StringIO()

    if len(table) < 1:
        return

    max_columns_widths = []
    num_columns = len(columns_names)
    for i in range(num_columns):
        max_value_len = max([len(str(row[i])) for row in table])
        column_name_len = len(columns_names[i])
        max_columns_widths.append(2 + max([max_value_len, column_name_len]))

    header_line = ""
    for i, name in enumerate(columns_names):
        width = max_columns_widths[i]
        header_line += f'|{name.center(width)}'
    header_line += '|'

    print('-' * len(header_line), file=f)
    print(header_line, file=f)
    print('-' * len(header_line), file=f)

    for row in table:
        row_line = ""
        for i, value in enumerate(row):
            width = max_columns_widths[i]
            row_line += f'|{str(value).center(width)}'
        row_line += '|'
        print(row_line, file=f)

    print('-' * len(header_line), file=f)

    return f.getvalue()


def get_delta_str(delta_value):
    abs_delta_str = num_bytes_for_display(abs(delta_value))
    if delta_value >= 0:
        return f"(+{abs_delta_str})"
    else:
        return f"(-{abs_delta_str})"


def get_growth_str(start_size_bytes, end_size_bytes, end_num_files):
    start_size_str = num_bytes_for_display(start_size_bytes)

    if end_size_bytes is not None:
        if start_size_bytes == end_size_bytes:
            if start_size_bytes > 0:
                value_str = f"{start_size_str} (No Change) " \
                            f"  [{end_num_files} Files]"
            else:
                value_str = "Empty Level"
        else:
            end_size_str = num_bytes_for_display(end_size_bytes)
            delta = end_size_bytes - start_size_bytes
            delta_str = get_delta_str(delta)
            value_str = \
                f"{start_size_str} -> {end_size_str}  {delta_str}" \
                f"  [{end_num_files} Files]"
    else:
        # End size is unknown
        value_str = f"{start_size_str} -> (UNKNOWN SIZE)"

    return value_str


def prepare_total_growth_info_for_display(total_growth_info):
    assert isinstance(total_growth_info, calc_utils.GrowthInfo)

    if total_growth_info == calc_utils.GrowthInfo():
        return "Can't Calculate"

    return get_growth_str(total_growth_info.start_size_bytes,
                          total_growth_info.end_size_bytes,
                          total_growth_info.end_num_files)


def prepare_cfs_growth_info_for_display(cfs_growth_info):
    cfs_disp = {}

    if not cfs_growth_info:
        return utils.NO_GROWTH_INFO_TEXT

    for cf_name in cfs_growth_info:
        if cfs_growth_info[cf_name] is None:
            cfs_disp[cf_name] = utils.NO_GROWTH_INFO_TEXT
            continue

        cfs_disp[cf_name] = {}

        if not cfs_growth_info[cf_name]:
            cfs_disp[cf_name] = utils.NO_GROWTH_INFO_TEXT
            continue

        total_bytes_start = 0
        total_bytes_end = None
        total_num_files = None

        # The levels are not ordered within growth[cf_name]
        levels_and_sizes = list(cfs_growth_info[cf_name].items())
        levels_and_sizes.sort()

        for level, growth_info in levels_and_sizes:
            start_size_bytes = growth_info.start_size_bytes
            end_size_bytes = growth_info.end_size_bytes
            end_num_files = growth_info.end_num_files

            if start_size_bytes is None:
                start_size_bytes = 0

            cfs_disp[cf_name][f"Level {level}"] = \
                get_growth_str(
                    start_size_bytes, end_size_bytes, end_num_files)

            total_bytes_start += start_size_bytes
            if end_size_bytes is not None:
                total_bytes_end = utils.accumulate(total_bytes_end,
                                                   end_size_bytes)
            total_num_files = utils.accumulate(total_num_files, end_num_files)

        cfs_disp[cf_name]["Sum"] =\
            get_growth_str(total_bytes_start, total_bytes_end, total_num_files)

    return cfs_disp


def prepare_db_ingest_info_for_display(ingest_info):
    assert isinstance(ingest_info, calc_utils.DbIngestInfo)

    disp = {}
    if not ingest_info:
        return "No Ingest Info"

    disp["Ingest"] = utils.get_human_readable_num_bytes(ingest_info.ingest)
    disp["Ingest Rate"] = f"{ingest_info.ingest_rate_mbps} MBps"
    disp["Ingest Time"] = ingest_info.time

    return disp


def prepare_live_files_info_for_display(live_files_info):
    assert isinstance(live_files_info, calc_utils.DbLiveFilesInfo)

    disp = {}
    if not live_files_info or live_files_info.total_size_bytes == 0:
        return "No Live Files Info"

    index_percentage_of_total = \
        (live_files_info.total_index_size_bytes /
         live_files_info.total_size_bytes) * 100
    filter_percentage_of_total = \
        (live_files_info.total_filter_size_bytes /
         live_files_info.total_size_bytes) * 100
    disp["Num Files"] = live_files_info.num_files
    disp["Total Size (Uncompressed)"] =\
        num_bytes_for_display(live_files_info.total_size_bytes)
    disp["Index Blocks"] =  \
        f"{num_bytes_for_display(live_files_info.total_index_size_bytes)} " \
        f" ({index_percentage_of_total:.1f}%)"
    disp["Filter Blocks"] =  \
        f"{num_bytes_for_display(live_files_info.total_filter_size_bytes)} " \
        f" ({filter_percentage_of_total:.1f}%)"

    return disp


def prepare_files_compression_info_for_display(files_compression_info):
    if not files_compression_info:
        return "No Compressed Files Info"

    disp = {}

    for compression_type, compression_info in files_compression_info.items():
        assert isinstance(compression_info,
                          calc_utils.DbFilesCompressionTypeInfo)

        compression_ratio =\
            compression_info.total_compressed_size_bytes /\
            compression_info.total_uncompressed_size_bytes * 100
        disp_compressed_size_bytes = \
            num_bytes_for_display(
                compression_info.total_compressed_size_bytes)
        disp_uncompressed_size_bytes = \
            num_bytes_for_display(
                compression_info.total_uncompressed_size_bytes)

        disp[compression_type] = {
            "Num Compressed Files": compression_info.num_files,
            "Compression Ratio":
                f"{compression_ratio:.1f}% "
                f"({disp_compressed_size_bytes} / "
                f"{disp_uncompressed_size_bytes})"}

    return disp


def prepare_seek_stats_for_display(seek_stats):
    assert isinstance(seek_stats, calc_utils.SeekStats)

    disp = dict()
    disp["Num Seeks"] = num_for_display(seek_stats.num_seeks)
    disp["Num Found Seeks"] = num_for_display(seek_stats.num_found_seeks)
    disp["Num Nexts"] = num_for_display(seek_stats.num_nexts)
    disp["Num Prevs"] = num_for_display(seek_stats.num_prevs)
    disp["Avg. Seek Range Size"] = f"{seek_stats.avg_seek_range_size:.1f}"
    disp["Avg. Seeks Rate Per Second"] = \
        num_for_display(seek_stats.avg_seek_rate_per_second)
    disp["Avg. Seek Latency"] = f"{seek_stats.avg_seek_latency_us:.1f} us"

    return disp


def prepare_cache_id_options_for_display(options):
    assert isinstance(options, cache_utils.CacheOptions)

    disp = dict()

    disp["Capacity"] = num_bytes_for_display(options.cache_capacity_bytes)
    disp["Num Shards"] = 2 ** options.num_shard_bits
    disp["Shard Size"] = num_bytes_for_display(options.shard_size_bytes)
    disp["CF-s"] = \
        {cf_name: asdict(cf_options) for cf_name, cf_options in
         options.cfs_specific_options.items()}

    return disp


def prepare_block_stats_of_cache_for_display(block_stats):
    assert isinstance(block_stats, db_files.BlockLiveFileStats)

    disp = dict()

    disp["Total Size"] = \
        num_bytes_for_display(block_stats.curr_total_live_size_bytes)
    disp["Avg. Size"] = \
        num_bytes_for_display(int(block_stats.get_avg_block_size()))
    disp["Max Size"] = \
        num_bytes_for_display(block_stats.largest_block_size_bytes)
    disp["Max Size At"] = block_stats.largest_block_size_time

    return disp


def prepare_block_cache_info_for_display(cache_info):
    assert isinstance(cache_info, cache_utils.CacheIdInfo)

    disp = dict()
    disp.update(prepare_cache_id_options_for_display(cache_info.options))

    blocks_stats = cache_info.files_stats.blocks_stats
    disp["Index Block"] = \
        prepare_block_stats_of_cache_for_display(
            blocks_stats[db_files.BlockType.INDEX])
    if blocks_stats[db_files.BlockType.FILTER].num_created > 0:
        disp["Filter Block"] = \
            prepare_block_stats_of_cache_for_display(
                blocks_stats[db_files.BlockType.FILTER])
    else:
        disp["Filter Block"] = "No Stats (Filters not in use)"

    return disp


def prepare_block_counters_for_display(cache_counters):
    assert isinstance(cache_counters, cache_utils.CacheCounters)

    disp = asdict(cache_counters)
    disp = {key: num_for_display(value) for key, value in disp.items()}
    return disp


# TODO: The detailed display should be moved to its own csv
# TODO: The cache id in the detailed should not include the process id so it
#  matches the non-detailed display
def prepare_detailed_block_cache_stats_for_display(detailed_block_cache_stats):
    disp_stats = detailed_block_cache_stats.copy()
    for cache_stats in disp_stats.values():
        cache_stats['Capacity'] = \
            num_bytes_for_display(cache_stats['Capacity'])
        cache_stats['Usage'] = num_bytes_for_display(cache_stats['Usage'])
        for cache_stats_key, entry in cache_stats.items():
            if utils.parse_time_str(cache_stats_key, expect_valid_str=False):
                entry['Usage'] = num_bytes_for_display(entry['Usage'])
                for entry_key, role_values in entry.items():
                    if entry_key == 'CF-s':
                        for cf_name in entry['CF-s']:
                            for role, cf_role_value in \
                                    entry['CF-s'][cf_name].items():
                                entry['CF-s'][cf_name][role] =\
                                    num_bytes_for_display(cf_role_value)
                    elif entry_key != 'Usage':
                        role_values['Size'] =\
                            num_bytes_for_display(role_values['Size'])
    return disp_stats


def prepare_block_cache_stats_for_display(cache_stats,
                                          detailed_block_cache_stats):
    assert isinstance(cache_stats, cache_utils.CacheStats)

    disp = dict()
    if cache_stats.per_cache_id_info:
        disp["Caches"] = {}
        for cache_id, cache_info in cache_stats.per_cache_id_info.items():
            disp["Caches"][cache_id] = \
                prepare_block_cache_info_for_display(cache_info)

    if cache_stats.global_cache_counters:
        disp["DB Counters"] = \
            prepare_block_counters_for_display(
                cache_stats.global_cache_counters)
    else:
        disp["DB Counters"] = utils.NO_COUNTERS_DUMPS_TEXT

    detailed_disp_block_cache_stats = None
    if detailed_block_cache_stats:
        detailed_disp_block_cache_stats = \
            prepare_detailed_block_cache_stats_for_display(
                detailed_block_cache_stats)
    if detailed_disp_block_cache_stats:
        disp["Detailed"] = detailed_disp_block_cache_stats
    else:
        disp["Detailed"] = "No Detailed Block Cache Stats Available"

    return disp


def prepare_cf_filter_stats_for_display(cf_filter_stats, format_as_dict):
    if cf_filter_stats.filter_policy:
        assert isinstance(cf_filter_stats, calc_utils.CfFilterFilesStats)
        if cf_filter_stats.filter_policy != utils.INVALID_FILTER_POLICY:
            sanitized_filter_policy = \
                SanitizedValueType.get_type_from_str(
                    cf_filter_stats.filter_policy)
            if sanitized_filter_policy == SanitizedValueType.NULL_PTR:
                cf_disp_stats = "No Filter"
            else:
                if cf_filter_stats.avg_bpk is not None:
                    bpk_str = f"{cf_filter_stats.avg_bpk:.1f}"
                else:
                    bpk_str = "unknown bpk"
                if format_as_dict:
                    cf_disp_stats = {
                        "Filter-Policy": cf_filter_stats.filter_policy,
                        "Avg. BPK": bpk_str
                    }
                else:
                    cf_disp_stats = \
                        f"{cf_filter_stats.filter_policy} ({bpk_str})"
        else:
            cf_disp_stats = "Filter Data Not Available"
    else:
        cf_disp_stats = "Filter Data Not Available"

    return cf_disp_stats


def prepare_filter_stats_for_display(filter_stats):
    assert isinstance(filter_stats, calc_utils.FilterStats)

    disp = {}

    if filter_stats.files_filter_stats:
        disp["CF-s"] = {}
        for cf_name, cf_stats in filter_stats.files_filter_stats.items():
            assert isinstance(cf_stats, calc_utils.CfFilterFilesStats)
            disp["CF-s"][cf_name] = \
                prepare_cf_filter_stats_for_display(cf_stats,
                                                    format_as_dict=True)
    else:
        disp["CF-s"] = "No Filters used In SST-s"

    if filter_stats.filter_counters and not \
            filter_stats.filter_counters.are_all_zeroes():
        disp_counters = {
            "False-Positive-Rate":
                f"1 in {filter_stats.filter_counters.one_in_n_fpr}",
            "False-Positives":
                num_for_display(filter_stats.filter_counters.false_positives),
            "Negatives":
                num_for_display(filter_stats.filter_counters.negatives),
            "True-Positives":
                num_for_display(filter_stats.filter_counters.true_positives)
        }
        disp["Counters"] = disp_counters
    else:
        disp["Counters"] = "No Filter Counters Available"

    return disp


def prepare_filter_effectiveness_stats_for_display(
        cfs_names, db_opts, counters_mngr, files_monitor):
    assert isinstance(db_opts, db_options.DatabaseOptions)
    assert isinstance(counters_mngr, CountersMngr)
    assert isinstance(files_monitor, db_files.DbFilesMonitor)

    filter_stats = calc_utils.calc_filter_stats(
        cfs_names, db_opts, files_monitor, counters_mngr)

    if filter_stats:
        return prepare_filter_stats_for_display(filter_stats)
    else:
        return "No Filter Stats Available"


def prepare_get_histogram_for_display(counters_mngr):
    assert isinstance(counters_mngr, CountersMngr)

    get_counter_name = "rocksdb.db.get.micros"
    get_histogram = \
        counters_mngr.get_last_histogram_entry(
            get_counter_name, non_zero=True)

    if get_histogram:
        return CountersMngr.\
            get_histogram_entry_display_values(get_histogram)
    else:
        logging.info("No Get latency histogram (maybe no stats)")
        return "No Get Info"


def prepare_multi_get_histogram_for_display(counters_mngr):
    assert isinstance(counters_mngr, CountersMngr)

    multi_get_counter_name = "rocksdb.db.multiget.micros"

    multi_get_histogram = \
        counters_mngr.get_last_histogram_entry(
            multi_get_counter_name, non_zero=True)

    if multi_get_histogram:
        return counters_mngr.\
            get_histogram_entry_display_values(multi_get_histogram)
    else:
        logging.info("No Multi-Get latency histogram (maybe no stats)")
        return "No Multi-Get Info"


def prepare_per_cf_read_latency_for_display(cf_file_histogram_stats_mngr):
    per_cf_stats = \
        calc_utils.calc_read_latency_per_cf_stats(cf_file_histogram_stats_mngr)

    stats = dict()

    if per_cf_stats:
        for cf_name, cf_stats in per_cf_stats.items():
            stats[cf_name] = {
                "Num Reads": num_for_display(cf_stats.num_reads),
                "Avg. Read Latency": f"{cf_stats.avg_read_latency_us:.1f} us",
                "Max Read Latency": f"{cf_stats.max_read_latency_us:.1f} us",
                "Read % of All CF-s":
                    f"{cf_stats.read_percent_of_all_cfs:.1f}%"
            }

    return stats


def prepare_applicable_read_stats(
        cfs_names, db_opts, counters_mngr, stats_mngr, files_monitor):
    assert isinstance(db_opts, db_options.DatabaseOptions)
    assert isinstance(counters_mngr, CountersMngr)
    assert isinstance(stats_mngr, StatsMngr)
    assert isinstance(files_monitor, db_files.DbFilesMonitor)

    stats = dict()
    stats["Get Histogram"] = prepare_get_histogram_for_display(counters_mngr)
    stats["Multi-Get Histogram"] = \
        prepare_multi_get_histogram_for_display(counters_mngr)

    cf_file_histogram_stats_mngr =\
        stats_mngr.get_cf_file_histogram_stats_mngr()
    stats["Per CF Read Latency"] = \
        prepare_per_cf_read_latency_for_display(cf_file_histogram_stats_mngr)

    stats["Filter Effectiveness"] = \
        prepare_filter_effectiveness_stats_for_display(
            cfs_names, db_opts, counters_mngr, files_monitor)

    return stats if stats else None


def prepare_mem_reps_for_display(mem_reps):
    assert isinstance(mem_reps, dict)

    disp = dict()
    for time, rep in mem_reps.items():
        # Remove all arena entities that do not use any memory
        arena_stats = \
            utils.delete_dict_keys_matching_value(rep.arena_stats, "0")
        disp_arena_stats = {
            "Total": rep.arena_total,
            "Entities": arena_stats
        }

        disp_cfs_stats = {
            "Total": rep.cfs_total,
            "CF-s": rep.cfs_stats
        }

        disp[time] = {
            "Arena": disp_arena_stats,
            "CF-s": disp_cfs_stats,
            "Misc": rep.misc_stats
        }

    return disp
