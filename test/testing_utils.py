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

import utils
from events import Event
from log_entry import LogEntry
from log_file import ParsedLog
from test.sample_log_info import SampleLogInfo
from dataclasses import dataclass


def read_file(file_path):
    with open(file_path, "r") as f:
        return f.readlines()


def create_parsed_log(file_path):
    log_lines = read_file(file_path)
    return ParsedLog(SampleLogInfo.FILE_PATH, log_lines,
                     should_init_baseline_info=False)


def entry_msg_to_entry(time_str, msg, process_id="0x123456", code_pos=None):
    if code_pos is not None:
        line = f"{time_str} {process_id} {code_pos} {msg}"
    else:
        line = f"{time_str} {process_id} {msg}"

    assert LogEntry.is_entry_start(line)
    return LogEntry(0, line, last_line=True)


def line_to_entry(line, last_line=True):
    assert LogEntry.is_entry_start(line)
    return LogEntry(0, line, last_line)


def lines_to_entries(lines):
    entries = []
    entry = None
    for i, line in enumerate(lines):
        if LogEntry.is_entry_start(line):
            if entry:
                entries.append(entry.all_lines_added())
            entry = LogEntry(i, line)
        else:
            assert entry
            entry.add_line(line)

    if entry:
        entries.append(entry.all_lines_added())

    return entries


def add_stats_entry_lines_to_counters_mngr(entry_lines, mngr):
    entry = LogEntry(0, entry_lines[0])
    for line in entry_lines[1:]:
        entry.add_line(line)
    mngr.add_entry(entry.all_lines_added())


def create_event_entry(job_id, time_str, event_type=None, cf_name=None,
                       make_illegal_json=False, **kwargs):
    event_line = time_str + " "
    event_line += '7f4a8b5bb700 EVENT_LOG_v1 {"time_micros": '

    event_line += str(utils.get_gmt_timestamp_us(time_str))

    if event_type is not None:
        event_line += f', "event": "{str(event_type.value)}"'
    if cf_name is not None:
        event_line += f', "cf_name": "{cf_name}"'
    if job_id is not None:
        event_line += f', "job": {job_id}'

    for k, v in kwargs.items():
        if isinstance(v, str):
            event_line += f', "{k}": "{v}"'
        elif isinstance(v, dict):
            event_line += f', "{k}":' + ' {'
            first_key = True
            for k1, v1 in v.items():
                if not first_key:
                    event_line += ", "
                if isinstance(v1, str):
                    event_line += f'"{k1}": "{v1}"'
                else:
                    event_line += f'"{k1}": {v1}'
                first_key = False

            event_line += "}"
            pass
        else:
            event_line += f', "{k}": {v}'

    if make_illegal_json:
        event_line += ", "

    event_line += '}'

    event_entry = LogEntry(0, event_line, True)
    assert Event.is_an_event_entry(event_entry)
    return event_entry


def entry_to_event(event_entry):
    assert Event.is_an_event_entry(event_entry)
    return Event(event_entry)


def create_event(job_id, cf_names, time_str, event_type=None, cf_name=None,
                 make_illegal_json=False, **kwargs):
    event_entry = create_event_entry(job_id, time_str, event_type, cf_name,
                                     make_illegal_json, **kwargs)

    assert Event.is_an_event_entry(event_entry)
    return Event.create_event(event_entry)


@dataclass
class TablePropertiesTestVars:
    event_time_micros: int = 0

    compressed_data_size_bytes: int = 62396458
    num_data_blocks: int = 1000
    total_keys_sizes_bytes: int = 2000
    total_values_sizes_bytes: int = 3333
    index_size_bytes: int = 3000
    filter_size_bytes: int = 4000
    num_entries: int = 5555
    filter_policy: str = "bloomfilter"
    num_filter_entries: int = 6666
    compression_type: str = "NoCompression"


def get_table_properties(table_properties_test_vars):
    assert isinstance(table_properties_test_vars, TablePropertiesTestVars)

    return {
        "compressed_data_size":
            table_properties_test_vars.compressed_data_size_bytes,
        "index_size": table_properties_test_vars.index_size_bytes,
        "index_partitions": 0,
        "top_level_index_size": 0,
        "index_key_is_user_key": 1,
        "index_value_is_delta_encoded": 1,
        "filter_size": table_properties_test_vars.filter_size_bytes,
        "raw_key_size": table_properties_test_vars.total_keys_sizes_bytes,
        "raw_average_key_size": 24,
        "raw_value_size": table_properties_test_vars.total_values_sizes_bytes,
        "raw_average_value_size": 1000,
        "num_data_blocks": table_properties_test_vars.num_data_blocks,
        "num_entries": table_properties_test_vars.num_entries,
        "num_filter_entries": table_properties_test_vars.num_filter_entries,
        "num_deletions": 0,
        "num_merge_operands": 0,
        "num_range_deletions": 0,
        "format_version": 0,
        "fixed_key_len": 0,
        "filter_policy": table_properties_test_vars.filter_policy,
        "column_family_name": "default",
        "column_family_id": 0,
        "comparator": "leveldb.BytewiseComparator",
        "merge_operator": "nullptr",
        "prefix_extractor_name": "nullptr",
        "property_collectors": "[]",
        "compression": table_properties_test_vars.compression_type,
        "oldest_key_time": 1672823099,
        "file_creation_time": 1672823099,
        "slow_compression_estimated_data_size": 0,
        "fast_compression_estimated_data_size": 0,
        "db_id": "c100448c-dc04-4c74-8ab2-65d72f3aa3a8",
        "db_session_id": "4GAWIG5RIF8PQWM3NOQG",
        "orig_file_number": 37155}


def get_time_series(start_time, time_diff_seconds, num):
    time_series = list()

    prev_time = start_time
    for i in range(num):
        next_time = utils.get_time_relative_to(prev_time, time_diff_seconds)
        time_series.append(next_time)
        prev_time = time_series[i]

    return time_series
