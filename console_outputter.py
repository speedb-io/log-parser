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

import display_utils
import log_file
import utils
from log_file import ParsedLog


def get_title(log_file_path, parsed_log):
    return f"Parsing of: {log_file_path}"


def print_title(f, log_file_path, parsed_log):
    title = get_title(log_file_path, parsed_log)
    print(f"{title}", file=f)
    print(len(title) * "=", file=f)


def print_cf_console_printout(f, parsed_log, db_size_msg_suffix):
    assert isinstance(parsed_log, log_file.ParsedLog)

    cfs_info_for_display = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    cfs_display_values = []
    for cf_name, cf_info in cfs_info_for_display.items():
        row = [
            cf_name,
            cf_info["CF Size"],
            cf_info["Avg. Key Size"],
            cf_info["Avg. Value Size"],
            cf_info["Compaction Style"],
            cf_info["Compression"],
            cf_info["Filter-Policy"]
        ]
        cfs_display_values.append(row)

    size_suffix = ""
    if db_size_msg_suffix is not None:
        size_suffix = f"({db_size_msg_suffix})"

    table_header = ["Column Family",
                    f"Size {size_suffix}",
                    "Avg. Key Size",
                    "Avg. Value Size",
                    "Compaction Style",
                    "Compression",
                    "Filter-Policy"]
    ascii_table = display_utils.generate_ascii_table(table_header,
                                                     cfs_display_values)
    print(ascii_table, file=f)


def print_general_info(f, parsed_log: ParsedLog):
    disp_dict = \
        display_utils.prepare_db_wide_info_for_display(parsed_log)

    if isinstance(disp_dict["Error Messages"], dict):
        error_lines = ""
        for error_time, error_msg in \
                disp_dict["Error Messages"].items():
            error_lines += f"\n{error_time} {error_msg}"
        disp_dict["Error Messages"] = error_lines

    if isinstance(disp_dict["Fatal Messages"], dict):
        error_lines = ""
        for error_time, error_msg in \
                disp_dict["Fatal Messages"].items():
            error_lines += f"\n{error_time} {error_msg}"
        disp_dict["Fatal Messages"] = error_lines

    suffix = ""

    msg1 = None
    db_size_msg_suffix = None
    db_size_time = disp_dict["DB Size Time"]
    if disp_dict["DB Size Time"] is not None:
        suffix += "*"
        db_size_msg_suffix = suffix
        disp_dict = \
            utils.replace_key_keep_order(disp_dict,
                                         "DB Size",
                                         f"DB Size ({suffix})")
        msg1 = f"({suffix}) Data is calculated at: {db_size_time}"
    del disp_dict["DB Size Time"]

    msg2 = None
    ingest_time = disp_dict["Ingest Time"]
    if ingest_time is not None:
        if db_size_time != ingest_time:
            suffix += "*"
            msg2 = f"({suffix}) Ingest Data are calculated at: {ingest_time}"
        disp_dict = \
            utils.replace_key_keep_order(
                disp_dict, "Ingest", f"Ingest ({suffix})")
    del disp_dict["Ingest Time"]

    msg3 = None
    num_cfs_info_key = "Num CF-s Info"
    if "Num CF-s Info" in disp_dict:
        suffix += "*"
        disp_dict = \
            utils.replace_key_keep_order(
                disp_dict, "Num CF-s", f"Num CF-s ({suffix})")
        num_cfs_info = disp_dict[num_cfs_info_key]
        msg3 = f"({suffix}) {num_cfs_info}"
        del disp_dict[num_cfs_info_key]

    width = 25
    for field_name, value in disp_dict.items():
        print(f"{field_name.ljust(width)}: {value}", file=f)
    print_cf_console_printout(f, parsed_log, db_size_msg_suffix)

    if msg1 is not None:
        print(msg1, file=f)
    if msg2 is not None:
        print(msg2, file=f)
    if msg3 is not None:
        print(msg3, file=f)


def get_console_output(log_file_path, parsed_log, output_type):
    logging.debug(f"Preparing {output_type} Console Output")

    f = io.StringIO()

    if output_type == utils.ConsoleOutputType.SHORT:
        print_title(f, log_file_path, parsed_log)
        print_general_info(f, parsed_log)

    return f.getvalue()
