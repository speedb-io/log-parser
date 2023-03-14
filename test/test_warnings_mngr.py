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
from log_entry import LogEntry
from warnings_mngr import WarningCategory, WarningElementInfo, WarningsMngr


def create_warning_entry(warning_type, cf_name, warning_element_info):
    assert isinstance(warning_type, utils.WarningType)
    assert isinstance(warning_element_info, WarningElementInfo)

    warning_line = warning_element_info.time + " "
    warning_line += '7f4a8b5bb700 '

    warning_line += f'[{warning_type.name}] '
    warning_line += f'[{warning_element_info.code_pos}] '

    if cf_name != utils.NO_CF:
        warning_line += f'[{cf_name}] '
    warning_line += warning_element_info.warning_msg

    warning_entry = LogEntry(0, warning_line, True)
    assert warning_entry.is_warn_msg()
    assert warning_entry.get_warning_type() == warning_type
    assert warning_entry.get_code_pos() == warning_element_info.code_pos

    return warning_entry


def add_warning(mngr, warning_type, time, code_pos, cf_name, warn_msg):
    assert isinstance(warning_type, utils.WarningType)

    warning_element_info = WarningElementInfo(time, code_pos, warn_msg)
    warn_entry = create_warning_entry(
        warning_type, cf_name, warning_element_info)
    assert mngr.try_adding_entry(warn_entry)


def test_non_warnings_entries():
    line1 = '''2022/04/17-14:21:50.026058 7f4a8b5bb700 [/flush_job.cc:333]
    [cf1] [JOB 9] Flushing memtable with next log file: 5'''

    line2 = '''2022/04/17-14:21:50.026087 7f4a8b5bb700 EVENT_LOG_v1
    {"time_micros": 1650205310026076, "job": 9, "event": "flush_started"'''

    cfs_names = ["cf1", "cf2"]
    warnings_mngr = WarningsMngr()

    assert LogEntry.is_entry_start(line1)
    entry1 = LogEntry(0, line1, True)
    assert LogEntry.is_entry_start(line2)
    entry2 = LogEntry(0, line2, True)

    assert not warnings_mngr.try_adding_entry(entry1)
    assert not warnings_mngr.try_adding_entry(entry2)
    warnings_mngr.set_cfs_names_on_parsing_complete(cfs_names)


def test_warn_entries_empty():
    mngr = WarningsMngr()
    mngr.set_cfs_names_on_parsing_complete(["cf1", "cf2"])
    assert mngr.get_all_warnings() == {}


def test_warn_entries_basic():
    cf1 = "cf1"
    cf2 = "cf2"
    cf3 = "cf3"
    cfs_names = [cf1, cf2, cf3]

    time1 = "2022/04/17-14:21:50.026058"
    time2 = "2022/04/17-14:21:51.026058"
    time3 = "2022/04/17-14:21:52.026058"
    time4 = "2022/04/17-14:21:53.026058"
    time5 = "2022/04/17-14:21:54.026058"

    warn_msg1 = "Warning Message 1"
    cf_warn_msg1 = f"[{cf1}] {warn_msg1}"
    warn_msg2 = "Warning Message 2"
    cf_warn_msg2 = f"[{cf2}] {warn_msg2}"
    warn_msg3 = "Warning Message 3"
    cf_warn_msg3 = f"[{cf2}] {warn_msg3}"

    delay_msg = "Stalling writes, L0 files 2, memtables 2"
    cf_delay_msg = f"[{cf2}] {delay_msg}"
    stop_msg = "Stopping writes Dummy Text 1"
    cf_stop_msg = f"[{cf1}] {stop_msg}"

    code_pos1 = "/flush_job.cc:333"
    code_pos2 = "/column_family.cc:932"
    code_pos3 = "/column_family1.cc:999"
    code_pos4 = "/column_family2.cc:1111"

    mngr = WarningsMngr()
    add_warning(mngr, utils.WarningType.WARN, time1, code_pos1, cf1, warn_msg1)
    add_warning(mngr, utils.WarningType.ERROR, time2,
                code_pos1, cf2, warn_msg2)
    add_warning(mngr, utils.WarningType.WARN, time3, code_pos2, cf2, warn_msg3)
    add_warning(mngr, utils.WarningType.WARN, time4, code_pos3, cf2, delay_msg)
    add_warning(mngr, utils.WarningType.WARN, time5, code_pos4, cf1, stop_msg)
    mngr.set_cfs_names_on_parsing_complete(cfs_names)

    expected_cf1_warn_warnings = {
        WarningCategory.WRITE_STOP:
            [WarningElementInfo(time5, code_pos4, cf_stop_msg)],
        WarningCategory.OTHER:
            [WarningElementInfo(time1, code_pos1, cf_warn_msg1)]
    }
    expected_cf2_warn_warnings = {
        WarningCategory.WRITE_DELAY:
            [WarningElementInfo(time4, code_pos3, cf_delay_msg)],
        WarningCategory.OTHER:
            [WarningElementInfo(time3, code_pos2, cf_warn_msg3)]
    }
    expected_cf2_error_warnings = {
        WarningCategory.OTHER:
            [WarningElementInfo(time2, code_pos1, cf_warn_msg2)]}

    expected_warn_warnings = {
            cf1: expected_cf1_warn_warnings,
            cf2: expected_cf2_warn_warnings
    }
    expected_error_warnings = {cf2: expected_cf2_error_warnings}

    all_expected_warnings = {
        utils.WarningType.WARN: expected_warn_warnings,
        utils.WarningType.ERROR: expected_error_warnings
    }

    actual_cf1_warn_warnings = mngr.get_cf_warn_warnings(cf1)
    assert actual_cf1_warn_warnings == expected_cf1_warn_warnings

    actual_cf2_warn_warnings = mngr.get_cf_warn_warnings(cf2)
    assert actual_cf2_warn_warnings == expected_cf2_warn_warnings

    actual_cf2_error_warnings = mngr.get_cf_error_warnings(cf2)
    assert actual_cf2_error_warnings == expected_cf2_error_warnings

    assert mngr.get_cf_warn_warnings(cf3) is None
    assert mngr.get_cf_error_warnings(cf3) is None

    actual_warn_warnings = mngr.get_warn_warnings()
    assert expected_warn_warnings == actual_warn_warnings

    all_actual_warnings = mngr.get_all_warnings()
    assert all_actual_warnings == all_expected_warnings
