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
import csv
import io
import logging
from dataclasses import dataclass

import utils
from events import FlowType, EventField


def get_counters_csv(counter_and_histograms_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    mngr = counter_and_histograms_mngr
    # Get all counters for which at least one entry is not 0 (=> there is at
    # least one value that should be included for them in the CSV)
    all_applicable_entries = mngr.get_counters_entries_not_all_zeroes()

    if not all_applicable_entries:
        logging.info("No counters with non-zero values => NO CSV")
        return None

    counters_names = list(all_applicable_entries.keys())
    times = mngr.get_counters_times()

    # Support counter entries with missing entries for some time point
    # Maintain an index per counter that advances only when the counter has
    # a value per csv row (one row per time point)
    counters_idx = {name: 0 for name in counters_names}

    # csv header line (counter names)
    writer.writerow(["Time"] + counters_names)

    # Write one line per time:
    for time_idx, time in enumerate(times):
        csv_line = list()
        csv_line.append(time)
        for counter_name in counters_names:
            counter_entries = all_applicable_entries[counter_name]

            value = 0
            if counters_idx[counter_name] < len(counter_entries):
                counter_entry_time =\
                    counter_entries[counters_idx[counter_name]]["time"]
                time_diff = \
                    utils.compare_times_strs(counter_entry_time, time)
                assert time_diff >= 0
                if time_diff == 0:
                    value =\
                        counter_entries[counters_idx[counter_name]]["value"]
                    counters_idx[counter_name] += 1

            csv_line.append(value)

        writer.writerow(csv_line)

    return f.getvalue()


def get_human_readable_histogram_csv(counter_and_histograms_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    mngr = counter_and_histograms_mngr
    # Get all histograms for which at least one entry is not 0 (=> there is at
    # least one value that should be included for them in the CSV)
    all_applicable_entries = mngr.get_histogram_entries_not_all_zeroes()

    if not all_applicable_entries:
        logging.info("No Histograms with non-zero values => NO CSV")
        return None

    counters_names = list(all_applicable_entries.keys())
    times = mngr.get_histogram_counters_times()

    # Support histogram entries with missing entries for some time point
    # Maintain an index per histogram that advances only when the histogram has
    # a value per csv row (one row per time point)
    histograms_idx = {name: 0 for name in counters_names}

    # csv header lines (counter names)
    header_line1 = [""]
    header_line2 = [""]

    counter_histogram_columns =\
        list(all_applicable_entries[counters_names[0]][0]["values"].keys())
    counter_histogram_columns.remove("Average")
    counter_histogram_columns.remove("Interval Count")
    counter_histogram_columns.remove("Interval Sum")

    num_counter_columns = len(counter_histogram_columns)
    for counter_name in counters_names:
        name_columns = ["." for i in range(num_counter_columns-1)]
        name_columns.insert(0, counter_name)
        # name_columns[int(num_counter_columns/2)] = counter_name
        header_line1.extend(name_columns)

        header_line2.extend(counter_histogram_columns)

    writer.writerow(header_line1)
    writer.writerow(header_line2)

    # Write one line per time:
    zero_values = [0 for i in range(num_counter_columns)]
    for time_idx, time in enumerate(times):
        csv_line = list()
        csv_line.append(time)
        for counter_name in counters_names:
            histogram_entries = all_applicable_entries[counter_name]

            values = zero_values
            idx = histograms_idx[counter_name]
            if idx < len(histogram_entries):
                counter_entry_time = histogram_entries[idx]["time"]
                time_diff = \
                    utils.compare_times_strs(counter_entry_time, time)
                assert time_diff >= 0
                if time_diff == 0:
                    values = list(histogram_entries[idx]["values"].values())
                    histograms_idx[counter_name] += 1

            csv_line.extend(values)

        writer.writerow(csv_line)

    return f.getvalue()


def get_tools_histogram_csv(counter_and_histograms_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    mngr = counter_and_histograms_mngr
    # Get all histograms for which at least one entry is not 0 (=> there is at
    # least one value that should be included for them in the CSV)
    all_applicable_entries = mngr.get_histogram_entries_not_all_zeroes()

    if not all_applicable_entries:
        logging.info("No Histograms with non-zero values => NO CSV")
        return None

    counters_names = list(all_applicable_entries.keys())
    times = mngr.get_histogram_counters_times()

    # Support histogram entries with missing entries for some time point
    # Maintain an index per histogram that advances only when the histogram has
    # a value per csv row (one row per time point)
    histograms_idx = {name: 0 for name in counters_names}

    # csv header lines (counter names)
    header_line = ["Name", "Time"]
    counter_histogram_columns =\
        list(all_applicable_entries[counters_names[0]][0]["values"].keys())
    header_line.extend(counter_histogram_columns)
    num_counter_columns = len(counter_histogram_columns)
    writer.writerow(header_line)

    # Write one line per time:
    zero_values = [0 for i in range(num_counter_columns)]
    for counter_name in counters_names:
        for time_idx, time in enumerate(times):
            csv_line = [counter_name, time]
            histogram_entries = all_applicable_entries[counter_name]

            values = zero_values
            idx = histograms_idx[counter_name]
            if idx < len(histogram_entries):
                counter_entry_time = histogram_entries[idx]["time"]
                time_diff = \
                    utils.compare_times_strs(counter_entry_time, time)
                assert time_diff >= 0
                if time_diff == 0:
                    values = list(histogram_entries[idx]["values"].values())
                    histograms_idx[counter_name] += 1

                csv_line.extend(values)

            writer.writerow(csv_line)

    return f.getvalue()


def get_compaction_stats_csv(compaction_stats_mngr):
    f = io.StringIO()
    writer = csv.writer(f)

    entries = compaction_stats_mngr.get_level_entries()

    if not entries:
        logging.info("No Compaction Stats => NO CSV")
        return None

    temp = list(list(entries.values())[0].values())[0]
    columns_names = list(list(temp.values())[0].keys())
    header_line = ["Time", "Column Family", "Level"] + columns_names
    writer.writerow(header_line)

    for time, time_entry in entries.items():
        for cf_name, cf_entry in time_entry.items():
            for level, level_values in cf_entry.items():
                row = [time, cf_name, level]
                row += list(level_values.values())
                writer.writerow(row)

    return f.getvalue()


def get_flow_events_csv(cfs_names, events_mngr, flow_type):
    f = io.StringIO()
    writer = csv.writer(f)

    immutable_events = events_mngr.get_all_flow_events(flow_type, cfs_names)
    if not immutable_events:
        return None

    # Going to modify the events so make a modifiable copy first
    events = copy.deepcopy(immutable_events)
    first_event = True
    for event_pair in events:
        start_event = event_pair[0]
        finish_event = event_pair[1]
        start_event_data = start_event.get_event_data_dict()
        cf_name = start_event.get_cf_name()
        event_start_time = start_event.get_log_time()

        if not finish_event:
            event_finish_time = "UNKNOWN"
            event_data = start_event_data
        else:
            event_finish_time = finish_event.get_log_time()
            finish_event_data = finish_event.get_event_data_dict()
            event_data = utils.unify_dicts(
                start_event_data, finish_event_data, favor_first=True)

        fields_to_del = [EventField.CF_NAME,
                         EventField.TIME_MICROS,
                         EventField.EVENT_TYPE]
        utils.delete_dict_keys(event_data, fields_to_del)

        if first_event:
            first_event = False
            event_columns_names = list(list(event_data.keys()))
            header_line = ["Start Time", "Finish Time", "Column Family"] + \
                event_columns_names
            writer.writerow(header_line)

        row = [event_start_time, event_finish_time, cf_name]
        row += list(event_data.values())
        writer.writerow(row)

    return f.getvalue()


@dataclass
class CompactionsCsvInputFilesInfo:
    updated_columns_names: list = None
    first_column_idx: int = None
    first_level: int = None
    second_level: int = None


def process_compactions_csv_header(columns_names):
    # Assume that, in general, compactions potentially have 2 "files_" columns
    # (They may have one, and, maybe more than 2)
    # Name them:
    # 1. The first: "Input Level Files"
    # 2. The second: "Input Files from Output Level"
    prefix = "files_L"
    prefix_len = len(prefix)

    updated_columns_names = copy.deepcopy(columns_names)
    input_files_columns = \
        utils.find_list_items_matching_prefix(updated_columns_names, prefix)

    if not input_files_columns:
        return None

    if len(input_files_columns) > 2:
        logging.warning(
            f"Compactions have more than 2 'files_' columns. Including only "
            f"the first 2. columns_names:{columns_names}")
        for to_remove in input_files_columns[2:]:
            updated_columns_names.remove(to_remove)
        input_files_columns = input_files_columns[:2]

    def extract_level(column_idx):
        level_str = columns_names[column_idx][prefix_len:]
        try:
            return int(level_str)
        except ValueError:
            logging.warning(f"Unexpected column name ("
                            f"{columns_names[column_idx]}")
            return None

    first_column_idx = updated_columns_names.index(input_files_columns[0])
    first_level = extract_level(first_column_idx)
    if first_level is None:
        return None
    updated_columns_names[first_column_idx] = "Input Level Files"

    second_level = None
    if len(input_files_columns) > 1:
        second_column_idx = updated_columns_names.index(input_files_columns[1])
        if second_column_idx != first_column_idx+1:
            # Currently, support only consecutive columns
            logging.warning(
                f"non-consecutive file_<Level> columns ({columns_names})")
            return None

        second_level = extract_level(second_column_idx)
        if not second_level:
            return None
        updated_columns_names[second_column_idx] = \
            "Input Files from Output Level"
    else:
        updated_columns_names.insert(first_column_idx + 1,
                                     "Input Files from Output Level")

    return CompactionsCsvInputFilesInfo(
        updated_columns_names=updated_columns_names,
        first_column_idx=first_column_idx,
        first_level=first_level,
        second_level=second_level
    )


def get_compactions_csv(compactions_monitor):
    # return get_flow_events_csv(events_mngr, FlowType.COMPACTION)
    f = io.StringIO()
    writer = csv.writer(f)

    jobs = compactions_monitor.get_finished_jobs()
    if not jobs:
        return None

    updated_header_columns_info = None
    for job_id, job_info in jobs.items():
        # Skipping incomplete jobs
        if not job_info.has_finished():
            logging.info("Compaction job hasn't finished, Not including in "
                         "csv (skipping).\n{job_info}")
            continue

        start_event = job_info.start_event
        finish_event = job_info.finish_event

        job_info_dict = {}
        if job_info.pre_finish_info:
            job_info_dict = job_info.pre_finish_info.as_dict()

        job_info_dict = utils.unify_dicts(job_info_dict,
                                          start_event.get_event_data_dict(),
                                          favor_first=True)
        job_info_dict = \
            utils.unify_dicts(job_info_dict,
                              finish_event.get_event_data_dict(),
                              favor_first=True)

        fields_to_del = [EventField.CF_NAME,
                         EventField.TIME_MICROS,
                         EventField.EVENT_TYPE,
                         EventField.RECORDS_IN,
                         EventField.RECORDS_DROPPED]
        utils.delete_dict_keys(job_info_dict, fields_to_del)

        columns_names = list(list(job_info_dict.keys()))
        if updated_header_columns_info is None:
            updated_header_columns_info = \
                process_compactions_csv_header(columns_names)
            if updated_header_columns_info is None:
                logging.warning("Failed processing CSV's header. Aborting")
                return None
            curr_updated_columns_info = updated_header_columns_info

            header_line = ["Start Time", "Finish Time", "Column Family"] + \
                updated_header_columns_info.updated_columns_names
            writer.writerow(header_line)
        else:
            curr_updated_columns_info = \
                process_compactions_csv_header(columns_names)
            if updated_header_columns_info.first_column_idx != \
                    curr_updated_columns_info.first_column_idx:
                logging.warning(
                    f"Mismatching compaction job fields. "
                    f"Skipping entry:{job_info}")
                continue

        job_values = list(job_info_dict.values())
        first_idx = curr_updated_columns_info.first_column_idx
        first_level_str = f"Level{curr_updated_columns_info.first_level}: "
        job_values[first_idx] = first_level_str + str(job_values[first_idx])

        if curr_updated_columns_info.second_level is not None:
            second_level_str = \
                f"Level{curr_updated_columns_info.second_level}: "
            job_values[first_idx+1] = \
                second_level_str + str(job_values[first_idx+1])
        else:
            job_values.insert(curr_updated_columns_info.first_column_idx+1, "")
        row = [job_info.get_start_time(),
               job_info.get_finish_time(),
               job_info.cf_name]
        row += job_values
        writer.writerow(row)

    if updated_header_columns_info is None:
        return None

    return f.getvalue()


def get_flushes_csv(cfs_names, events_mngr):
    return get_flow_events_csv(cfs_names, events_mngr, FlowType.FLUSH)


def generate_counters_csv(mngr, output_folder, report_to_console):
    counters_csv = get_counters_csv(mngr)

    if counters_csv:
        counters_csv_path = \
            utils.get_counters_csv_file_path(output_folder)
        with open(counters_csv_path, "w") as f:
            f.write(counters_csv)
        msg_start = "Counters CSV Is in "
        utils.print_msg(
            f"{msg_start}{counters_csv_path}", report_to_console,
            f"{msg_start}{counters_csv_path.as_uri()}")
        return counters_csv_path
    else:
        utils.print_msg("No Counters to report", report_to_console)
        return None


def generate_human_readable_histograms_csv(mngr, output_folder,
                                           report_to_console):
    histograms_csv = \
        get_human_readable_histogram_csv(mngr)
    if not histograms_csv:
        utils.print_msg("No Counters Histograms to report", report_to_console)
        return None

    histograms_csv_file_name = \
        utils. \
        get_human_readable_histograms_csv_file_path(output_folder)
    with open(histograms_csv_file_name, "w") as f:
        f.write(histograms_csv)
    msg_start = "Human Readable Counters Histograms CSV Is in "
    utils.print_msg(
        f"{msg_start}{histograms_csv_file_name}", report_to_console,
        f"{msg_start}{histograms_csv_file_name.as_uri()}")
    return histograms_csv_file_name


def generate_tools_histograms_csv(mngr, output_folder, report_to_console):
    histograms_csv = get_tools_histogram_csv(mngr)
    if not histograms_csv:
        logging.info("No Counters Histograms to report")
        return None

    histograms_csv_file_name = \
        utils.get_tools_histograms_csv_file_path(output_folder)
    with open(histograms_csv_file_name, "w") as f:
        f.write(histograms_csv)
    logging.info(f"Tools Counters Histograms CSV Is in"
                 f" {histograms_csv_file_name}")
    return histograms_csv_file_name


def generate_histograms_csv(mngr, output_folder, report_to_console):
    human_readable_csv_file_path = \
        generate_human_readable_histograms_csv(
            mngr, output_folder, report_to_console)
    if human_readable_csv_file_path is None:
        return None, None

    tools_csv_file_path = generate_tools_histograms_csv(
        mngr, output_folder, report_to_console)

    return human_readable_csv_file_path, tools_csv_file_path


def generate_compactions_stats_csv(compaction_stats_mngr, output_folder,
                                   report_to_console):
    compaction_stats_csv = get_compaction_stats_csv(compaction_stats_mngr)
    if compaction_stats_csv is None:
        utils.print_msg("No Compaction Stats to report", report_to_console)
        return None

    compactions_stats_csv_path = \
        utils.get_compactions_stats_csv_file_path(output_folder)
    with open(compactions_stats_csv_path, "w") as f:
        f.write(compaction_stats_csv)
    msg_start = "Compactions Stats CSV Is in "
    utils.print_msg(
        f"{msg_start}{compactions_stats_csv_path}", report_to_console,
        f"{msg_start}{compactions_stats_csv_path.as_uri()}")
    return compactions_stats_csv_path


def generate_compactions_csv(
        compactions_monitor, output_folder, report_to_console):
    compaction_csv = get_compactions_csv(compactions_monitor)
    if compaction_csv is None:
        utils.print_msg("No Compactions to report", report_to_console)
        return None

    compactions_csv_path = \
        utils.get_compactions_csv_file_path(output_folder)
    with open(compactions_csv_path, "w") as f:
        f.write(compaction_csv)
    msg_start = "Compactions CSV Is in "
    utils.print_msg(
        f"{msg_start}{compactions_csv_path}", report_to_console,
        f"{msg_start}{compactions_csv_path.as_uri()}")
    return compactions_csv_path


def generate_flushes_csv(
        cfs_names, events_mngr, output_folder, report_to_console):
    flushes_csv = get_flushes_csv(cfs_names, events_mngr)
    if flushes_csv is None:
        utils.print_msg("No Flushes to report", report_to_console)
        return None

    flushes_csv_path = utils.get_flushes_csv_file_path(output_folder)
    with open(flushes_csv_path, "w") as f:
        f.write(flushes_csv)
    msg_start = "Flushes CSV Is in "
    utils.print_msg(
        f"{msg_start}{flushes_csv_path}", report_to_console,
        f"{msg_start}{flushes_csv_path.as_uri()}")
    return flushes_csv_path
