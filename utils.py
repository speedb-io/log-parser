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

""" Common constants and utilities used in the log parser's modules """

import copy
import logging
import pathlib
import re
import time
from calendar import timegm
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

import regexes

MIN_PYTHON_VERSION_MAJOR = 3
MIN_PYTHON_VERSION_MINOR = 8

NO_CF = 'DB_WIDE'
INVALID_CF = "UNKNOWN-CF"
DEFAULT_CF_NAME = "default"

INVALID_CF_ID = -1
INVALID_JOB_ID = -1
INVALID_FILE_NUMBER = -1
INVALID_LEVEL = -1
INVALID_FILTER_POLICY = "INVALID-FILTER-POLICY"

DB_WIDE_WRITE_BUFFER_MANAGER_OPTIONS_NAME = "write_buffer_manager"

BASELINE_LOGS_FOLDER = "baseline_logs"
DIFF_BASELINE_NAME = "Baseline"
DIFF_LOG_NAME = "Parsed Log"

DEFAULT_OUTPUT_FOLDER = "output_files"
OUTPUT_SUB_FOLDER_PREFIX = "run_"
DEFAULT_LOG_FILE_NAME = "log_parser.log"
DEFAULT_JSON_FILE_NAME = "log.json"
DEFAULT_COUNTERS_FILE_NAME = "counters.csv"
DEFAULT_HUMAN_READABLE_HISTOGRAMS_FILE_NAME = "histograms_human_readable.csv"
DEFAULT_TOOLS_HISTOGRAMS_FILE_NAME = "histograms_tools.csv"
DEFAULT_COMPACTIONS_STATS_FILE_NAME = "compactions_stats.csv"
DEFAULT_COMPACTIONS_FILE_NAME = "compactions.csv"
DEFAULT_FLUSHES_FILE_NAME = "flushes.csv"

FILE_NOT_GENERATED_TEXT = "File Not Generated"
DATA_UNAVAILABLE_TEXT = "Data Unavailable"
UNKNOWN_VALUE_TEXT = "UNKNOWN"
NO_INGEST_TEXT = "No Ingest Info Available"
NO_STATS_TEXT = "No Statistics"
NO_COUNTERS_DUMPS_TEXT = "No Counters Dumps Available"
NO_FLUSHES_TEXT = "No Flush Started Events"
NO_SEEKS_TEXT = "No Seeks"
NO_WARNS_TEXT = "No Warnings"
NO_READS_TEXT = "No Reads"
NO_COMPACTIONS_TEXT = "No Compactions"
NO_BLOCK_CACHE_STATS = "No Block Cache Statistics"
NO_GROWTH_INFO_TEXT = "No Growth Information Available"


# =====================================
#           MISC UTILS
# =====================================


def get_last_dict_entry(d):
    assert d is None or isinstance(d, dict)

    if not d:
        return None
    key = list(d.keys())[-1]
    return {key: d[key]}


def get_first_dict_entry_components(d):
    assert d is None or isinstance(d, dict)

    if not d:
        return None
    key = list(d.keys())[0]
    return key, d[key]


def get_last_dict_entry_components(d):
    assert d is None or isinstance(d, dict)

    if not d:
        return None
    key = list(d.keys())[-1]
    return key, d[key]


def delete_dict_keys(in_dict, keys_to_delete):
    """ Delete specific keys from an dictionary"""
    for key in keys_to_delete:
        if key in in_dict:
            del in_dict[key]


def unify_dicts(dict1, dict2, favor_first):
    # Avoid mutating the input dictionary
    unified_dict = copy.deepcopy(dict1)

    for key, value in dict2.items():
        if key in unified_dict:
            if not favor_first:
                unified_dict[key] = dict2[key]
        else:
            unified_dict[key] = value

    return unified_dict


def replace_key_keep_order(d, existing_key, new_key):
    if existing_key not in d:
        return

    pos = list(d.keys()).index(existing_key)
    value = d[existing_key]

    items = list(d.items())
    items.insert(pos, (new_key, value))
    updated_d = dict(items)
    del(updated_d[existing_key])

    return updated_d


def insert_dict_entry_before_key(d, existing_key, new_key, new_value):
    if existing_key not in d:
        return

    pos = list(d.keys()).index(existing_key)
    value = d[existing_key]

    items = list(d.items())
    items.insert(pos, (new_key, value))
    updated_d = dict(items)

    return updated_d


def insert_dict_entry_after_key(d, existing_key, new_key, new_value):
    if existing_key not in d:
        return

    pos = list(d.keys()).index(existing_key)
    value = d[existing_key]

    items = list(d.items())
    items.insert(pos, (new_key, value))
    updated_d = dict(items)

    return updated_d


def find_dict_keys_matching_prefix(d, key_prefix):
    assert isinstance(d, dict)
    matching = list()
    for key in d.keys():
        if isinstance(key, str) and key.startswith(key_prefix):
            matching.append(key)
    return matching


def find_list_items_matching_prefix(lst, prefix):
    assert isinstance(lst, list)
    return [e for e in lst if isinstance(e, str) and e.startswith(prefix)]


def are_dicts_equal_and_in_same_keys_order(d1, d2):
    if d1 != d2:
        return False
    return list(d1.keys()) == list(d2.keys())


def unify_lists_preserve_order(l1, l2):
    # Unifies the lists, removes duplicates, but maintains the order
    unified_with_duplicates = l1 + l2
    seen = set()
    return [x for x in unified_with_duplicates
            if not (x in seen or seen.add(x))]


def sort_dict_on_values(d):
    return dict(sorted(d.items(), key=lambda x: x[1]))


def has_method(obj, method_name):
    """ Checks if a method exists in an obj """
    return method_name in dir(obj)


def get_gmt_timestamp_us(time_str):
    """
    Converts a time string in the log's format to a GMT Unix Timestamp.
    The resolution is in seconds

    Example: '2018/07/25-11:25:45.782710' will be converted into 1532517945
    """
    hr_time = time_str + 'GMT'
    dt = datetime.strptime(hr_time,  "%Y/%m/%d-%H:%M:%S.%f%Z")
    us = dt.microsecond
    epoch_seconds = timegm(time.strptime(hr_time, "%Y/%m/%d-%H:%M:%S.%f%Z"))
    return epoch_seconds * 10**6 + us


def get_time_relative_to(base_time, num_seconds, num_us=0):
    base_dt = datetime.strptime(base_time + 'GMT',  "%Y/%m/%d-%H:%M:%S.%f%Z")
    num_seconds = num_seconds + num_us / 10**6
    dt = timedelta(seconds=num_seconds)
    rel_time_dt = base_dt + dt
    return rel_time_dt.strftime("%Y/%m/%d-%H:%M:%S.%f%Z")


def get_times_strs_diff(time_str1, time_str2):
    """ Calculates the difference between 2 log time string """
    dt1 = datetime.strptime(time_str1 + 'GMT',  "%Y/%m/%d-%H:%M:%S.%f%Z")
    dt2 = datetime.strptime(time_str2 + 'GMT',  "%Y/%m/%d-%H:%M:%S.%f%Z")
    return dt2 - dt1


def get_times_strs_diff_seconds(time_str1, time_str2):
    return get_times_strs_diff(time_str1, time_str2).total_seconds()


def compare_times_strs(time1_str, time2_str):
    """ Compares 2 log time strings

    Returns:
        -1: time1_str < time2_str
        0: time1_str == time2_str
        1: time1_str > time2_str
    """
    diff_total_seconds = get_times_strs_diff_seconds(time2_str, time1_str)
    if diff_total_seconds < 0:
        return -1
    elif diff_total_seconds > 0:
        return 1
    else:
        return 0


def convert_seconds_to_dd_hh_mm_ss(seconds):
    seconds = int(seconds)
    days = int(seconds / 86400)
    return time.strftime(f'{days}d %Hh %Mm %Ss', time.gmtime(seconds))


# =====================================
#       LOGGING TYPES & UTILS
# =====================================

@dataclass
class ParsingContext:
    file_path: str = None
    parsing_done: bool = False
    line_idx: int = 0
    line: str = None

    def parsing_starts(self, file_path):
        self.file_path = file_path

    def parsing_ends(self):
        assert not self.parsing_done
        self.parsing_done = True

    def is_parsing_done(self):
        return self.parsing_done

    def update_line(self, line_idx, new_line=None):
        self.line_idx = line_idx
        if new_line:
            self.line = new_line
        else:
            self.line = None

    def increment_line(self, increment=1, new_line=None):
        new_line_idx = self.line_idx + increment
        self.update_line(new_line_idx, new_line)


parsing_context = None


@dataclass
class ErrorContext:
    file_path: Optional[str] = None
    log_line_idx: Optional[int] = None
    log_line: Optional[str] = None

    def __str__(self):
        file_path = self.file_path if self.file_path is not None else "?"
        line_num = self.log_line_idx + 1 \
            if self.log_line_idx is not None else "?"

        result_str = f"[File:{file_path} (line#:{line_num})]"
        if self.log_line is not None:
            result_str += f"\n{self.log_line}"
        return result_str


def get_error_context_from_entry(entry, file_path=None):
    error_context = ErrorContext()
    if not error_context:
        error_context = ErrorContext()
    if file_path:
        error_context.file_path = file_path
    error_context.log_line = entry.get_msg()
    error_context.log_line_idx = entry.get_start_line_idx()
    return error_context


def format_err_msg(msg, error_context=None, entry=None, file_path=None):
    if entry:
        error_context = get_error_context_from_entry(entry, file_path)

    result_str = msg
    if error_context is not None:
        result_str += " - " + str(error_context)
    return result_str


def get_line_num_from_entry(entry, rel_line_idx=None):
    line_idx = entry.get_start_line_idx()
    if rel_line_idx is not None:
        line_idx += rel_line_idx
    return line_idx + 1


def format_line_num_from_line_idx(line_idx):
    return f"[line# {line_idx + 1}]"


def format_line_num_from_entry(entry, rel_line_idx=None):
    line_idx = entry.get_start_line_idx()
    if rel_line_idx is not None:
        line_idx += rel_line_idx
    return format_line_num_from_line_idx(line_idx)


def format_lines_range_from_entries(start_entry, end_entry):
    result = "[lines#"
    result += f"{start_entry.get_start_line_idx() + 1}"
    result += "-"
    result += f"{end_entry.get_end_line_idx() + 1}"
    result += "]"
    return result


def format_lines_range_from_entries_idxs(log_entries, start_idx, end_idx):
    return format_lines_range_from_entries(log_entries[start_idx],
                                           log_entries[end_idx])


def print_msg(msg, to_console=True, console_msg=None):
    logging.info(msg)
    if to_console:
        if console_msg:
            print(console_msg)
        else:
            print(msg)


# =====================================
#       EXCEPTIONS
# =====================================


class ParsingError(Exception):
    def __init__(self, msg, error_context=None):
        self.msg = msg
        self.context = error_context

    def __str__(self):
        result_str = self.msg
        if self.context is not None:
            result_str += str(self.context)
        return result_str

    def set_context(self, error_context):
        self.context = error_context


class ParsingAssertion(ParsingError):
    def __init__(self, msg, error_context=None):
        super().__init__(msg, error_context)


class LogFileNotFoundError(Exception):
    def __init__(self, file_path):
        self.msg = f"Log file to parse ({file_path}) Not Found"


class EmptyLogFile(Exception):
    def __init__(self, file_path):
        self.msg = f"{file_path} Is Empty"


class InvalidLogFile(Exception):
    def __init__(self, file_path):
        self.msg = f"{file_path} is not a valid log File"


class WarningType(str, Enum):
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


class ConsoleOutputType(str, Enum):
    SHORT = "short"
    LONG = "long"


class ProductName(str, Enum):
    ROCKSDB = "RocksDB"
    SPEEDB = "Speedb"

    def __eq__(self, other):
        return self.lower() == other.lower()


# =====================================
#       PARSING UTILITIES
# =====================================


def parse_time_str(time_str, expect_valid_str=True):
    try:
        return datetime.strptime(time_str, '%Y/%m/%d-%H:%M:%S.%f')
    except ValueError:
        if expect_valid_str:
            raise ParsingError(f"Invalid time str ({time_str}")
        return None


def is_valid_time_str(time_str):
    return parse_time_str(time_str, expect_valid_str=False) is not None


NUM_BYTES_UNITS_STRS = ["KB", "MB", "GB", "TB"]
NUM_UNITS_STRS = ["K", "M", "G"]


def __convert_human_readable_components(
        num_bytes_without_unit_str, size_units_str, units_list, factor):
    try:
        num_bytes_without_unit_str = float(num_bytes_without_unit_str)
    except ValueError:
        raise ParsingError(
            f"Num bytes is not a nummer: {num_bytes_without_unit_str}")

    size_units_str = size_units_str.strip()

    try:
        unit_idx = units_list.index(size_units_str)
        multiplier = factor ** (unit_idx + 1)
    except ValueError:
        if size_units_str != '':
            raise ParsingAssertion(
                f"Unexpected size units ({size_units_str}")
        multiplier = 1

    result = float(num_bytes_without_unit_str) * multiplier
    return int(result)


def get_num_bytes_from_human_readable_components(num_bytes_without_unit_str,
                                                 size_units_str):
    return __convert_human_readable_components(
        num_bytes_without_unit_str,
        size_units_str,
        NUM_BYTES_UNITS_STRS,
        1024)


def get_num_bytes_from_human_readable_str(size_with_unit_str):
    match = re.findall(f"{regexes.NUM_BYTES_WITH_UNIT_ONLY}",
                       size_with_unit_str)
    if not match:
        raise ParsingError(f"Invalid size with unit str ({size_with_unit_str}")

    size, size_unit = match[0]

    return get_num_bytes_from_human_readable_components(size, size_unit)


def get_human_readable_num_bytes(size_in_bytes):
    if size_in_bytes < 2 ** 10:
        return str(size_in_bytes) + " B"
    elif size_in_bytes < 2 ** 20:
        size_units_str = "KB"
        divider = 2 ** 10
    elif size_in_bytes < 2 ** 30:
        size_units_str = "MB"
        divider = 2 ** 20
    elif size_in_bytes < 2 ** 40:
        size_units_str = "GB"
        divider = 2 ** 30
    else:
        size_units_str = "TB"
        divider = 2 ** 40

    return f"{float(size_in_bytes) / divider:.1f} {size_units_str}"


def get_number_from_human_readable_components(num_bytes_without_unit_str,
                                              size_units_str):
    return __convert_human_readable_components(
        num_bytes_without_unit_str,
        size_units_str,
        NUM_UNITS_STRS,
        1000)


def get_number_from_human_readable_str(number_with_units_str):
    match = re.findall(f"{regexes.NUM_WITH_UNIT_ONLY}", number_with_units_str)
    if not match:
        raise ParsingError(
            f"Invalid size with unit str ({number_with_units_str}")

    size, size_unit = match[0]
    return get_number_from_human_readable_components(size, size_unit)


def get_human_readable_number(number):
    assert number >= 0

    if number < 10 ** 4:
        return str(number)
    elif number < 10 ** 7:
        size_units_str = "K"
        divider = 10**3
    elif number < 10 ** 10:
        size_units_str = "M"
        divider = 10**6
    else:
        size_units_str = "G"
        divider = 10**9

    return f"{float(number) / divider:.1f} {size_units_str}"


def get_num_leading_spaces(line):
    return len(line) - len(line.lstrip())


def remove_empty_lines_at_start(lines):
    return [line for line in lines if line.strip()]


def try_find_cfs_in_lines(cfs_names, lines):
    """ Try to find the first cf name in cfs_names that appears in lines
    cf names are searched as "[<cf-name>]"
    Returns either the cf-name(s) (if found) or None (if not)
    """
    if isinstance(lines, list):
        lines = "\n".join(lines)
    match = re.findall(regexes.CF_NAME_OLD, lines, re.MULTILINE)
    if not match:
        return None

    potential_cfs_names_set = set(match)
    cfs_names_set = set(cfs_names)
    found_cfs_names = list(potential_cfs_names_set.intersection(cfs_names_set))

    if not found_cfs_names:
        return None
    if len(found_cfs_names) == 1:
        return found_cfs_names[0]
    else:
        return found_cfs_names


# =====================================
#           FILE PATHS UTILS
# =====================================


def get_file_path(file_folder_name, file_basename):
    return pathlib.Path(f"{file_folder_name}/{file_basename}").resolve()


def get_json_file_path(output_folder, json_file_name):
    return get_file_path(output_folder, json_file_name)


def get_log_file_path(output_folder):
    return get_file_path(output_folder, DEFAULT_LOG_FILE_NAME)


def get_counters_csv_file_path(output_folder):
    return get_file_path(output_folder, DEFAULT_COUNTERS_FILE_NAME)


def get_human_readable_histograms_csv_file_path(output_folder):
    return get_file_path(output_folder,
                         DEFAULT_HUMAN_READABLE_HISTOGRAMS_FILE_NAME)


def get_tools_histograms_csv_file_path(output_folder):
    return get_file_path(output_folder,
                         DEFAULT_TOOLS_HISTOGRAMS_FILE_NAME)


def get_compactions_stats_csv_file_path(output_folder):
    return get_file_path(output_folder,
                         DEFAULT_COMPACTIONS_STATS_FILE_NAME)


def get_compactions_csv_file_path(output_folder):
    return get_file_path(output_folder,
                         DEFAULT_COMPACTIONS_FILE_NAME)


def get_flushes_csv_file_path(output_folder):
    return get_file_path(output_folder, DEFAULT_FLUSHES_FILE_NAME)
