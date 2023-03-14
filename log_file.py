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

import baseline_log_files_utils
import regexes
import utils
from cfs_infos import CfsMetadata
from compactions import CompactionsMonitor
from counters import CountersMngr
from db_files import DbFilesMonitor
from db_options import DatabaseOptions
from events import EventsMngr
from log_entry import LogEntry
from log_file_options_parser import LogFileOptionsParser
from stats_mngr import StatsMngr
from warnings_mngr import WarningsMngr

get_error_context = utils.get_error_context_from_entry


class LogFileMetadata:
    """
    Contains the metadata information about a log file:
    - Product Name (RocksDB / Speedb)
    - S/W Version
    - Git Hash
    - DB Session Id
    - Times of first and last log entries in the file
    """
    def __init__(self, log_entries, start_entry_idx):
        self.product_name = None
        self.version = None
        self.db_session_id = None
        self.git_hash = None
        self.start_time = None
        # Will be set later (when file is fully parsed)
        self.end_time = None

        if len(log_entries) == 0:
            logging.warning("Empty Metadata pars (no entries)")
            return

        self.start_time = log_entries[0].get_time()

        # Parsing all entries and searching for predefined metadata
        # entities. Some may or may not exist (e.g., the DB Session Id is
        # not present in rolled logs). Also, no fixed order is assumed.
        # However, it is a parsing error if the same entitiy is found more
        # than once
        for i, entry in enumerate(log_entries):
            if self.try_parse_as_product_and_version_entry(entry):
                continue
            elif self.try_parse_as_git_hash_entry(entry):
                continue
            elif self.try_parse_as_db_session_id_entry(entry):
                continue

    def __str__(self):
        start_time = self.start_time if self.start_time else "UNKNOWN"
        end_time = self.end_time if self.end_time else "UNKNOWN"
        return f"LogFileMetadata: Start:{start_time}, End:{end_time}"

    def try_parse_as_product_and_version_entry(self, log_entry):
        lines = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.PRODUCT_AND_VERSION, lines)

        if not match_parts or len(match_parts) != 1:
            return False

        if self.product_name or self.version:
            raise utils.ParsingError(
                    f"Product / Version already parsed. Product:"
                    f"{self.product_name}, Version:{self.version})."
                    f"\n{log_entry}")

        self.product_name, self.version = match_parts[0]
        return True

    def try_parse_as_git_hash_entry(self, log_entry):
        lines = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.GIT_HASH_LINE, lines)

        if not match_parts or len(match_parts) != 1:
            return False

        if self.git_hash:
            raise utils.ParsingError(
                f"Git Hash Already Parsed ({self.git_hash})\n{log_entry}")

        self.git_hash = match_parts[0]
        return True

    def try_parse_as_db_session_id_entry(self, log_entry):
        lines = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.DB_SESSION_ID, lines)

        if not match_parts or len(match_parts) != 1:
            return False

        if self.db_session_id:
            raise utils.ParsingError(
                f"DB Session Id Already Parsed ({self.db_session_id})"
                f"n{log_entry}")

        self.db_session_id = match_parts[0]
        return True

    def set_end_time(self, end_time):
        assert not self.end_time,\
            f"End time already set ({self.end_time})"
        assert utils.compare_times_strs(end_time, self.start_time) > 0

        self.end_time = end_time

    def is_valid(self):
        return self.product_name and self.version

    def get_product_name(self):
        return self.product_name

    def get_version(self):
        return self.version

    def get_git_hash(self):
        return self.git_hash

    def get_db_session_id(self):
        return self.db_session_id

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_log_time_span_seconds(self):
        if not self.end_time:
            raise utils.ParsingAssertion(f"Unknown end time.\n{self}")
        return int(utils.get_times_strs_diff_seconds(self.start_time,
                                                     self.end_time))


class ParsedLog:
    def __init__(self, log_file_path, log_lines, should_init_baseline_info):
        logging.debug(f"Starting to parse {log_file_path}")
        utils.parsing_context = utils.ParsingContext()
        utils.parsing_context.parsing_starts(log_file_path)

        self.log_file_path = log_file_path
        self.metadata = None
        self.db_options = DatabaseOptions()
        self.cfs_metadata = CfsMetadata(self.log_file_path)
        self.cfs_names = {}
        self.next_unknown_cf_name_suffix = None

        self.entry_idx = 0
        self.log_entries, self.job_id_to_cf_name_map = \
            self.parse_log_to_entries(log_file_path, log_lines)

        self.events_mngr = EventsMngr(self.job_id_to_cf_name_map)
        self.compactions_monitor = CompactionsMonitor()
        self.files_monitor = DbFilesMonitor()
        self.warnings_mngr = WarningsMngr()
        self.stats_mngr = StatsMngr()
        self.counters_mngr = CountersMngr()
        self.not_parsed_entries = []

        self.parse_metadata()
        self.set_end_time()
        self.parse_rest_of_log()

        # no need to take up the memory for that
        self.log_entries = None

        utils.parsing_context.parsing_ends()
        self.cfs_metadata.parsing_complete()
        self.warnings_mngr.set_cfs_names_on_parsing_complete(
            self.get_cfs_names(include_auto_generated=False))

        self.baseline_info = None
        if should_init_baseline_info:
            self.init_baseline_info()

        logging.debug(f"Parsing of {self.log_file_path} Complete")

    def __str__(self):
        return f"ParsedLog ({self.log_file_path})"

    @staticmethod
    def parse_log_to_entries(log_file_path, log_lines):
        if len(log_lines) < 1:
            raise utils.EmptyLogFile(log_file_path)

        # first line must be the beginning of a log entry
        if not LogEntry.is_entry_start(log_lines[0]):
            raise utils.InvalidLogFile(log_file_path)

        logging.debug("Parsing log to entries")

        # Failure to parse an entry should just skip that entry
        # (best effort)
        log_entries = list()
        job_id_to_cf_name_map = dict()

        new_entry = None
        skip_until_next_entry_start = False
        for line_idx, line in enumerate(log_lines):
            try:
                if LogEntry.is_entry_start(line):
                    skip_until_next_entry_start = False
                    if new_entry:
                        log_entries.append(new_entry.all_lines_added())
                    new_entry = LogEntry(line_idx, line)
                    ParsedLog.add_job_id_to_cf_mapping_if_available(
                        new_entry, job_id_to_cf_name_map)
                else:
                    # To account for logs split into multiple lines
                    if new_entry:
                        new_entry.add_line(line)
                    else:
                        if not skip_until_next_entry_start:
                            raise utils.ParsingAssertion(
                                "Bug while parsing log to entries.",
                                log_file_path, line_idx)
            except utils.ParsingError as e:
                logging.error(str(e.value))
                # Discarding the "bad" entry and skipping all lines until
                # finding the start of the next one.
                new_entry = None
                skip_until_next_entry_start = True

        # Handle the last entry in the file.
        if new_entry:
            log_entries.append(new_entry.all_lines_added())

        logging.debug("Completed Parsing log to entries")

        return log_entries, job_id_to_cf_name_map

    @staticmethod
    def add_job_id_to_cf_mapping_if_available(entry, job_id_to_cf_name_map):
        job_id = entry.get_job_id()
        if not job_id:
            return

        cf_name = entry.get_cf_name()
        if not cf_name:
            return

        if job_id in job_id_to_cf_name_map:
            assert job_id_to_cf_name_map[job_id] == cf_name
        else:
            job_id_to_cf_name_map[job_id] = cf_name

    @staticmethod
    def find_next_options_entry(log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        while entry_idx < len(log_entries) and \
                not LogFileOptionsParser.is_options_entry(
                    log_entries[entry_idx]):
            entry_idx += 1

        return (entry_idx < len(log_entries)), entry_idx

    def parse_metadata(self):
        # Metadata must be at the top of the log and surely doesn't extend
        # beyond the first options line
        has_found, options_entry_idx = \
            ParsedLog.find_next_options_entry(self.log_entries, self.entry_idx)

        self.metadata = \
            LogFileMetadata(self.log_entries[self.entry_idx:options_entry_idx],
                            self.entry_idx)
        if not self.metadata.is_valid():
            raise utils.InvalidLogFile(self.log_file_path)

        self.entry_idx = options_entry_idx

    def generate_next_unknown_cf_name(self):
        # The first one is always "default" - NOT considered auto-generated
        if self.next_unknown_cf_name_suffix is None:
            self.next_unknown_cf_name_suffix = 1
            return False, utils.DEFAULT_CF_NAME
        else:
            next_cf_name = f"Unknown-CF-#{self.next_unknown_cf_name_suffix}"
            self.next_unknown_cf_name_suffix += 1
            return True, next_cf_name

    def parse_cf_options(self, cf_options_header_available):
        if cf_options_header_available:
            is_auto_generated = False
            auto_generated_cf_name = None
        else:
            is_auto_generated, auto_generated_cf_name = \
                self.generate_next_unknown_cf_name()

        cf_entry_idx = self.entry_idx
        cf_name, options_dict, table_options_dict, self.entry_idx, \
            duplicate_option = \
            LogFileOptionsParser.parse_cf_options(self.log_entries,
                                                  self.entry_idx,
                                                  auto_generated_cf_name)
        self.db_options.set_cf_options(cf_name,
                                       options_dict,
                                       table_options_dict)

        # TODO - Handle failure in add_cf_found_during_cf_options_parsing
        cf_id = None
        self.cfs_metadata.add_cf_found_during_cf_options_parsing(
            cf_name, cf_id, is_auto_generated, self.log_entries[cf_entry_idx])

    @staticmethod
    def find_support_info_start_index(log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        while entry_idx < len(log_entries):
            if re.findall(regexes.SUPPORT_INFO_START_LINE,
                          log_entries[entry_idx].get_msg_lines()[0]):
                return entry_idx
            entry_idx += 1

        raise utils.ParsingError(
            f"Failed finding Support Info. start-idx:{start_entry_idx}")

    def try_parse_as_cf_lifetime_entry(self):
        parse_result, self.entry_idx = \
            self.cfs_metadata.try_parse_as_cf_lifetime_entries(
                self.log_entries, self.entry_idx)
        return parse_result

    def are_dw_wide_options_set(self):
        return self.db_options.are_db_wide_options_set()

    def try_parse_as_db_wide_options(self):
        if self.are_dw_wide_options_set() or \
                not LogFileOptionsParser.is_options_entry(
                    self.get_curr_entry()):
            return False

        support_info_entry_idx = \
            ParsedLog.find_support_info_start_index(self.log_entries,
                                                    self.entry_idx)

        options_dict =\
            LogFileOptionsParser.parse_db_wide_options(self.log_entries,
                                                       self.entry_idx,
                                                       support_info_entry_idx)
        if not options_dict:
            raise utils.ParsingError(
                f"Empy db-wide options dictionary ({self}).",
                self.get_curr_error_context())

        self.db_options.set_db_wide_options(options_dict)
        self.entry_idx = support_info_entry_idx

        return True

    def try_parse_as_cf_options(self):
        entry = self.get_curr_entry()
        result = False
        if LogFileOptionsParser.is_cf_options_start_entry(entry):
            self.parse_cf_options(cf_options_header_available=True)
            result = True
        elif LogFileOptionsParser.is_options_entry(entry):
            assert self.are_dw_wide_options_set()
            self.parse_cf_options(cf_options_header_available=False)
            result = True

        return result

    def try_parse_as_warning_entries(self):
        entry = self.log_entries[self.entry_idx]

        result = self.warnings_mngr.try_adding_entry(entry)
        if result:
            self.entry_idx += 1

        return result

    def try_parse_as_event_entries(self):
        entry = self.get_curr_entry()

        result, event, cf_name = self.events_mngr.try_adding_entry(entry)
        if not result:
            return False

        self.add_cf_name_found_during_parsing(cf_name, entry)
        if event:
            self.compactions_monitor.new_event(event)
            self.files_monitor.new_event(event)

        self.entry_idx += 1

        return True

    def try_parse_as_stats_entries(self):
        entry_idx_on_entry = self.entry_idx
        result, self.entry_idx, cfs_names = \
            self.stats_mngr.try_adding_entries(self.log_entries,
                                               self.entry_idx)

        if result:
            self.add_cfs_names_found_during_parsing(
                cfs_names, self.get_entry(entry_idx_on_entry))

        return result

    def try_parse_as_counters_stats_entries(self):
        result, self.entry_idx = \
            self.counters_mngr.try_adding_entries(
                self.log_entries, self.entry_idx)

        return result

    def try_processing_in_monitors(self):
        curr_entry = self.get_curr_entry()
        processed, cf_name = \
            self.compactions_monitor.consider_entry(curr_entry)
        if cf_name:
            self.add_cf_name_found_during_parsing(cf_name, curr_entry)

        return processed

    def add_cf_name_found_during_parsing(self, cf_name, entry):
        if cf_name is None:
            return
        self.add_cfs_names_found_during_parsing([cf_name], entry)

    def add_cfs_names_found_during_parsing(self, cfs_names, entry):
        if not cfs_names:
            return
        for cf_name in cfs_names:
            self.cfs_metadata.handle_cf_name_found_during_parsing(
                cf_name, entry)

    def parse_rest_of_log(self):
        # Parse all the entries and process those that are required
        try:
            while self.entry_idx < len(self.log_entries):
                curr_entry_idx = self.entry_idx
                try:
                    if self.try_parse_as_cf_lifetime_entry():
                        continue

                    if self.try_parse_as_db_wide_options():
                        continue

                    if self.try_parse_as_cf_options():
                        continue

                    if self.try_parse_as_warning_entries():
                        continue

                    if self.try_parse_as_event_entries():
                        continue

                    if self.try_parse_as_stats_entries():
                        continue

                    if self.try_parse_as_counters_stats_entries():
                        continue

                    if not self.try_processing_in_monitors():
                        self.not_parsed_entries.append(self.get_curr_entry())

                    self.entry_idx += 1

                except utils.ParsingError:
                    logging.error("Error while parsing, skipping.")

                    # Make sure we are not stuck forever
                    if curr_entry_idx == self.entry_idx:
                        self.entry_idx += 1

        except AssertionError:
            logging.error(f"Assertion While Parsing {self.log_file_path}")
            raise

    def handle_cf_name_found_during_parsing(self, cf_name):
        if not self.cfs_metadata.handle_cf_name_found_during_parsing(
                cf_name, self.get_curr_entry()):
            return

    def init_baseline_info(self):
        self.baseline_info = \
            baseline_log_files_utils.get_baseline_database_options(
                utils.BASELINE_LOGS_FOLDER,
                self.metadata.get_product_name(),
                self.metadata.get_version())

    def get_start_time(self):
        return self.metadata.get_start_time()

    def set_end_time(self):
        last_entry = self.log_entries[-1]
        end_time = last_entry.get_time()
        self.metadata.set_end_time(end_time)

    def get_num_seconds_from_start(self, time_str):
        num_seconds =\
            utils.get_times_strs_diff_seconds(
                self.get_start_time(), time_str)
        if num_seconds < 0:
            logging.warning(
                f"time ({time_str}) is before log start\n{self.metadata}")
            return 0

        return num_seconds

    def get_log_file_path(self):
        return self.log_file_path

    def get_metadata(self):
        return self.metadata

    def get_cfs_names(self, include_auto_generated):
        if include_auto_generated:
            return self.cfs_metadata.get_all_cfs_names()
        else:
            return self.cfs_metadata.get_non_auto_generated_cfs_names()

    def get_cfs_names_that_have_options(self, include_auto_generated):
        # Return only the names of cf-s for which options exist
        return self.cfs_metadata.get_cfs_names_that_have_options(
            include_auto_generated)

    def get_auto_generated_cfs_names(self):
        return self.cfs_metadata.get_auto_generated_cf_names()

    def does_have_auto_generated_cfs_names(self):
        return len(self.get_auto_generated_cfs_names()) > 0

    def get_num_cfs_when_certain(self):
        return self.cfs_metadata.get_num_cfs_when_certain()

    def get_database_options(self):
        return self.db_options

    def get_events_mngr(self):
        return self.events_mngr

    def get_compactions_monitor(self):
        return self.compactions_monitor

    def get_stats_mngr(self):
        return self.stats_mngr

    def get_counters_mngr(self):
        return self.counters_mngr

    def get_files_monitor(self):
        return self.files_monitor

    def get_warnings_mngr(self):
        return self.warnings_mngr

    def get_entry(self, entry_idx):
        return self.log_entries[entry_idx]

    def get_curr_entry(self):
        if self.entry_idx < len(self.log_entries):
            return self.log_entries[self.entry_idx]
        else:
            return None

    def get_curr_error_context(self):
        curr_entry = self.get_curr_entry()
        if curr_entry is not None:
            return get_error_context(curr_entry, self.log_file_path)
        else:
            return None

    def get_baseline_info(self):
        return self.baseline_info
