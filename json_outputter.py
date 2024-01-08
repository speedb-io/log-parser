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
import json
import logging

import cache_utils
import calc_utils
import display_utils
import log_file
import utils


def get_general_json(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    general_json = display_utils.prepare_db_wide_info_for_display(parsed_log)
    if general_json["DB Size Time"] is None:
        del(general_json["DB Size Time"])
    if general_json["Ingest Time"] is None:
        del(general_json["Ingest Time"])

    return general_json


def get_db_size_json(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    stats_mngr = parsed_log.get_stats_mngr()
    db_wide_stats_mngr = stats_mngr.get_db_wide_stats_mngr()
    compactions_stats_mngr = stats_mngr.get_compactions_stats_mngr()
    files_monitor = parsed_log.get_files_monitor()

    db_size_json = {}

    ingest_info = calc_utils.get_db_ingest_info(db_wide_stats_mngr)
    if ingest_info:
        ingest_json = \
            display_utils.prepare_db_ingest_info_for_display(ingest_info)
        db_size_json["Ingest"] = ingest_json
    else:
        db_size_json["Ingest"] = utils.DATA_UNAVAILABLE_TEXT

    live_files_info = \
        calc_utils.get_live_files_info(files_monitor)
    if live_files_info:
        live_files_json = \
            display_utils.prepare_live_files_info_for_display(live_files_info)
        db_size_json["Live Files"] = live_files_json
    else:
        db_size_json["Live Files"] = utils.DATA_UNAVAILABLE_TEXT

    files_compression_info = \
        calc_utils.get_files_compression_info(files_monitor)
    if files_compression_info:
        files_compression_json = \
            display_utils.\
            prepare_files_compression_info_for_display(files_compression_info)
        db_size_json["Files Compression"] = files_compression_json
    else:
        db_size_json["Files Compression"] = utils.DATA_UNAVAILABLE_TEXT

    cfs_growth_info = \
        calc_utils.calc_cfs_growth_info(cfs_names,
                                        compactions_stats_mngr)
    num_included_cfs, num_cfs, total_growth_info = \
        calc_utils.calc_total_growth_info(cfs_growth_info)
    total_growth_json = \
        display_utils.prepare_total_growth_info_for_display(total_growth_info)
    if num_included_cfs == num_cfs:
        total_growth_key = "Total Growth (For All CF-s)"
    else:
        total_growth_key = \
            f"Total Growth (For {num_included_cfs}/{num_cfs} CF-s)"

        db_size_json[total_growth_key] = total_growth_json

    cfs_growth_json =\
        display_utils.prepare_cfs_growth_info_for_display(cfs_growth_info)
    db_size_json["CF-s Growth"] = cfs_growth_json

    return db_size_json


def get_flushes_json(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    flushes_json = {}

    flushes_stats =\
        display_utils.prepare_cf_flushes_stats_for_display(parsed_log)

    if flushes_stats:
        flushes_json["CF-s"] = {}
        cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
        for cf_name in cfs_names:
            if cf_name in flushes_stats:
                flushes_json["CF-s"][cf_name] = flushes_stats[cf_name]
    else:
        flushes_json = utils.NO_FLUSHES_TEXT

    return flushes_json


def get_compactions_json(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    compactions_json = {}

    compactions_stats = \
        display_utils.prepare_cf_compactions_stats_for_display(parsed_log)
    if compactions_stats:
        compactions_json.update(
            display_utils.prepare_global_compactions_stats_for_display(
                parsed_log))
        compactions_json["CF-s"] = {}
        cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
        for cf_name in cfs_names:
            if cf_name in compactions_stats:
                compactions_json["CF-s"][cf_name] = compactions_stats[cf_name]
    else:
        compactions_json = utils.NO_COMPACTIONS_TEXT

    return compactions_json


def get_reads_json(parsed_log):
    assert isinstance(parsed_log, log_file.ParsedLog)

    db_options = parsed_log.get_database_options()
    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    stats_mngr = parsed_log.get_stats_mngr()
    counters_mngr = parsed_log.get_counters_mngr()
    files_monitor = parsed_log.get_files_monitor()

    read_stats = \
        display_utils.prepare_applicable_read_stats(cfs_names,
                                                    db_options,
                                                    counters_mngr,
                                                    stats_mngr,
                                                    files_monitor)
    if read_stats:
        return read_stats
    else:
        return utils.NO_READS_TEXT


def get_seeks_json(parsed_log):
    counters_mngr = \
        parsed_log.get_counters_mngr()
    seek_stats = calc_utils.get_applicable_seek_stats(
        counters_mngr)
    if seek_stats:
        disp_dict = display_utils.prepare_seek_stats_for_display(seek_stats)
        return disp_dict
    else:
        return utils.NO_SEEKS_TEXT


def get_warn_warnings_json(cfs_names, warnings_mngr):
    warnings_info = calc_utils.get_warn_warnings_info(cfs_names, warnings_mngr)
    if warnings_info:
        disp_dict = \
            display_utils.prepare_warn_warnings_for_display(warnings_info)
        return disp_dict
    else:
        return utils.NO_WARNS_TEXT


def get_error_warnings_json(warnings_mngr):
    return display_utils.prepare_error_or_fatal_warnings_for_display(
        warnings_mngr, prepare_error=True)


def get_fatal_warnings_json(warnings_mngr):
    return display_utils.prepare_error_or_fatal_warnings_for_display(
        warnings_mngr, prepare_error=False)


def get_warnings_json(parsed_log):
    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    warnings_mngr = parsed_log.get_warnings_mngr()
    warnings_json = get_warn_warnings_json(cfs_names, warnings_mngr)
    return warnings_json


def get_block_cache_json(parsed_log):
    cache_stats = \
        cache_utils.calc_block_cache_stats(
            parsed_log.get_database_options(),
            parsed_log.get_counters_mngr(),
            parsed_log.get_files_monitor())
    if cache_stats:
        stats_mngr = parsed_log.get_stats_mngr()
        detailed_block_cache_stats = \
            stats_mngr.get_block_cache_stats_mngr().get_all_cache_entries()

        display_stats = \
            display_utils.prepare_block_cache_stats_for_display(
                cache_stats, detailed_block_cache_stats)
        return display_stats
    else:
        return utils.NO_BLOCK_CACHE_STATS


def get_mem_rep_json(parsed_log):
    mem_rep_mngr = parsed_log.get_mem_rep_mngr()
    mem_reps = mem_rep_mngr.get_reports()

    if mem_reps:
        display_reports = \
            display_utils.prepare_mem_reps_for_display(mem_reps)
        return display_reports
    else:
        return utils.NO_MEM_REPS


def get_json(parsed_log):
    j = dict()

    j["General"] = get_general_json(parsed_log)
    j["General"]["CF-s"] = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    j["Options"] = {
        "Diff":
            display_utils.get_options_baseline_diff_for_display(parsed_log),
        "All Options": display_utils.get_all_options_for_display(parsed_log)
    }

    j["DB-Size"] = get_db_size_json(parsed_log)

    j["Flushes"] = get_flushes_json(parsed_log)
    j["Compactions"] = get_compactions_json(parsed_log)
    j["Reads"] = get_reads_json(parsed_log)
    j["Seeks"] = get_seeks_json(parsed_log)
    j["Warnings"] = get_warnings_json(parsed_log)
    j["Block-Cache-Stats"] = get_block_cache_json(parsed_log)
    j["Memory-Reporting"] = get_mem_rep_json(parsed_log)

    return j


def get_json_dump_str(json_content):
    f = io.StringIO()
    json.dump(json_content, f, indent=1)
    return f.getvalue()


def write_json(json_file_name, json_content, output_folder, report_to_console):
    json_path = utils.get_json_file_path(output_folder, json_file_name)
    with json_path.open(mode='w') as json_file:
        json.dump(json_content, json_file)
    logging.info(f"JSON Output is in {json_path}")
    if report_to_console:
        print(f"JSON Output is in {json_path.as_uri()}")
