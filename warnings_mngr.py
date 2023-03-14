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

import logging
import re
from dataclasses import dataclass
from enum import Enum

import regexes
import utils
from log_entry import LogEntry


class WarningType(str, Enum):
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


@dataclass
class WarningElementInfo:
    time: str
    code_pos: str
    warning_msg: str

    def __str__(self):
        return f"{self.time} [{self.code_pos}] {self.warning_msg}"


class WarningCategory(str, Enum):
    WRITE_DELAY = "Write-Delay"
    WRITE_STOP = "Write-Stop"
    OTHER = "Other"


class WarningsMngr:
    def __init__(self):
        self.cfs_names = None
        # Stores all warnings during parsing (and before all cfs-names are
        # known)
        self.unprocessed_warnings = \
            {warning_type: list() for warning_type in WarningType}
        # Replaces unprocessed_warnings once parsing completes
        # Format:
        # {<warning-type>: {<cf-name>: {<category>: [warn-info]}}}

        self.processed_warnings = None

    def try_adding_entry(self, entry):
        assert isinstance(entry, LogEntry)

        if not entry.is_warn_msg():
            return False

        warning_type = entry.get_warning_type()
        warning_time = entry.get_time()
        code_pos = entry.get_code_pos()
        warning_msg = entry.get_msg()

        warning_info = WarningElementInfo(warning_time, code_pos, warning_msg)
        self.unprocessed_warnings[warning_type].append(warning_info)

        return True

    @staticmethod
    def is_write_delay_msg(warn_msg):
        return re.search(regexes.WRITE_DELAY_WARN_MSG, warn_msg.strip()) is \
               not None

    @staticmethod
    def is_write_stop_msg(warn_msg):
        return re.search(regexes.WRITE_STOP_WARN_MSG, warn_msg.strip()) is \
               not None

    @staticmethod
    def classify_warning_msg(msg):
        if WarningsMngr.is_write_delay_msg(msg):
            return WarningCategory.WRITE_DELAY
        elif WarningsMngr.is_write_stop_msg(msg):
            return WarningCategory.WRITE_STOP
        else:
            return WarningCategory.OTHER

    @staticmethod
    def determine_warning_msg_cf(cfs_names, msg):
        cfs_in_warn = utils.try_find_cfs_in_lines(cfs_names, msg.splitlines())

        if cfs_in_warn is None:
            return utils.NO_CF
        elif isinstance(cfs_in_warn, list):
            logging.info(f"Warning msg with multiple cfs. Can't determine. "
                         f"Placing in DB's bucket. cfs:{cfs_in_warn}\nmsg")
            return utils.NO_CF
        else:
            return cfs_in_warn

    def set_cfs_names_on_parsing_complete(self, cfs_names):
        assert self.processed_warnings is None

        self.cfs_names = cfs_names

        # Now process all the warnings
        # Processed warnings are organized as follows:
        # {<warning-type>: {<cf-name>: {<category>: [warn-info]}}}
        #
        self.processed_warnings = \
            {warning_type: dict() for warning_type in WarningType}

        for warning_type in self.unprocessed_warnings:
            for info in self.unprocessed_warnings[warning_type]:
                assert isinstance(info, WarningElementInfo)

                category = WarningsMngr.classify_warning_msg(info.warning_msg)
                assert isinstance(category, WarningCategory)

                cf_name = \
                    WarningsMngr.determine_warning_msg_cf(
                        self.cfs_names, info.warning_msg)
                assert cf_name is not None

                if cf_name not in self.processed_warnings[warning_type]:
                    self.processed_warnings[warning_type][cf_name] = dict()
                if category not in \
                        self.processed_warnings[warning_type][cf_name]:
                    self.processed_warnings[warning_type][cf_name][
                        category] = list()

                self.processed_warnings[warning_type][cf_name][
                    category].append(info)

        for warning_type in list(self.processed_warnings.keys()):
            if not self.processed_warnings[warning_type]:
                del(self.processed_warnings[warning_type])

        self.unprocessed_warnings = None

    def is_parsing_complete(self):
        return self.unprocessed_warnings is None

    def verify_parsing_complete(self):
        assert self.is_parsing_complete()

    def get_all_warnings(self):
        self.verify_parsing_complete()
        return self.processed_warnings

    def get_warnings_of_type(self, warning_type):
        assert isinstance(warning_type, WarningType)

        all_warnings = self.get_all_warnings()
        if not all_warnings:
            return None

        if warning_type not in all_warnings:
            return None

        return all_warnings[warning_type]

    def get_warn_warnings(self):
        return self.get_warnings_of_type(WarningType.WARN)

    def get_error_warnings(self):
        return self.get_warnings_of_type(WarningType.ERROR)

    def get_fatal_warnings(self):
        return self.get_warnings_of_type(WarningType.FATAL)

    def get_cf_warnings_of_type(self, cf_name, warning_type):
        all_warnings_of_type = self.get_warnings_of_type(warning_type)
        if not all_warnings_of_type:
            return None

        if cf_name not in all_warnings_of_type:
            return None

        return all_warnings_of_type[cf_name]

    def get_cf_warn_warnings(self, cf_name):
        return self.get_cf_warnings_of_type(cf_name, WarningType.WARN)

    def get_cf_error_warnings(self, cf_name):
        return self.get_cf_warnings_of_type(cf_name, WarningType.ERROR)

    def get_cf_fatal_warnings(self, cf_name):
        return self.get_cf_warnings_of_type(cf_name, WarningType.FATAL)

    def get_cf_warnings_of_type_and_category(
            self, cf_name, warning_type, category):
        assert isinstance(category, WarningCategory)

        all_cf_warnings_of_type = \
            self.get_cf_warnings_of_type(cf_name, warning_type)
        if not all_cf_warnings_of_type:
            return None

        if category not in all_cf_warnings_of_type:
            return None

        return all_cf_warnings_of_type[category]

    def get_total_num_of_type(self, warn_type):
        all_of_type = self.get_warnings_of_type(warn_type)
        if not all_of_type:
            return 0

        total_num = 0
        for cf_name, cf_data in all_of_type.items():
            for category in cf_data.keys():
                total_num += len(cf_data[category])

        return total_num

    def get_total_num_warns(self):
        return self.get_total_num_of_type(WarningType.WARN)

    def get_total_num_errors(self):
        return self.get_total_num_of_type(WarningType.ERROR)

    def get_total_num_fatals(self):
        return self.get_total_num_of_type(WarningType.FATAL)
