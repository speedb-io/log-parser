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

import regexes
import utils
from log_entry import LogEntry

format_lines_range_from_entries_idxs = \
    utils.format_lines_range_from_entries_idxs
format_line_num_from_entry = utils.format_line_num_from_entry


TABLE_OPTIONS_TOPIC_TITLES = [("metadata_cache_options", "metadata_cache_"),
                              ("block_cache_options", "block_cache_")]


def get_table_options_topic_info(topic_name):
    for topic_info in TABLE_OPTIONS_TOPIC_TITLES:
        if topic_info[0] == topic_name:
            return topic_info
    return None


class LogFileOptionsParser:
    @staticmethod
    def try_parsing_as_options_entry(log_entry):
        assert isinstance(log_entry, LogEntry)

        option_parts_match = re.findall(regexes.OPTION_LINE,
                                        log_entry.get_msg())
        if len(option_parts_match) != 1 or len(option_parts_match[0]) != 2:
            return None
        assert len(option_parts_match) == 1 or len(option_parts_match[0]) == 2

        option_name = option_parts_match[0][0].strip()
        option_value = option_parts_match[0][1].strip()

        return option_name, option_value

    @staticmethod
    def is_options_entry(line):
        if LogFileOptionsParser.try_parsing_as_options_entry(line):
            return True
        else:
            return False

    @staticmethod
    def try_parsing_as_table_options_entry(log_entry):
        assert isinstance(log_entry, LogEntry)

        options_lines = log_entry.get_non_stripped_msg_lines()
        if len(options_lines) < 1:
            # TODO - Maybe a bug - consider asserting
            return None

        options_dict = dict()
        # first line has the "table_factory options:" prefix
        # example:
        # options:   flush_block_policy_factory: FlushBlockBySizePolicyFactory
        option_parts_match = re.findall(regexes.TABLE_OPTIONS_START_LINE,
                                        options_lines[0])
        if len(option_parts_match) != 1 or len(option_parts_match[0]) != 2:
            return None
        options_dict[option_parts_match[0][0].strip()] = \
            option_parts_match[0][1].strip()

        options_lines_to_parse = options_lines[1:]
        line_idx = 0
        while line_idx < len(options_lines_to_parse):
            line = options_lines_to_parse[line_idx]
            option_name, option_value = \
                LogFileOptionsParser.parse_table_options_line(line)
            if option_name is None:
                line_idx += 1
                continue
            topic_info = \
                get_table_options_topic_info(option_name)
            if topic_info is None:
                options_dict[option_name] = option_value
                line_idx += 1
            else:
                line_idx = \
                    LogFileOptionsParser.parse_table_options_topic_options(
                        topic_info[1], options_lines_to_parse, line_idx,
                        options_dict)

        return options_dict

    @staticmethod
    def parse_table_options_line(line):
        option_parts_match = re.findall(
            regexes.TABLE_OPTIONS_CONTINUATION_LINE, line)
        if not option_parts_match:
            return None, None

        assert len(option_parts_match) == 1 and \
               len(option_parts_match[0]) == 2
        option_name = option_parts_match[0][0].strip()
        option_value = option_parts_match[0][1].strip()
        return option_name, option_value

    @staticmethod
    def parse_table_options_topic_options(topic_prefix,
                                          options_lines_to_parse, line_idx,
                                          options_dict):
        topic_line = options_lines_to_parse[line_idx]
        topic_line_indentation_size =\
            utils.get_num_leading_spaces(topic_line)
        line_idx += 1

        while line_idx < len(options_lines_to_parse):
            checked_line = options_lines_to_parse[line_idx]
            checked_line_indentation_size = \
                utils.get_num_leading_spaces(checked_line)
            if checked_line_indentation_size <=   \
                    topic_line_indentation_size:
                break
            option_name, option_value = \
                LogFileOptionsParser.parse_table_options_line(checked_line)
            if option_name is None:
                break
            options_dict[f"{topic_prefix}{option_name}"] = option_value
            line_idx += 1

        return line_idx

    @staticmethod
    def try_parsing_as_cf_options_start_entry(log_entry):
        parts = re.findall(regexes.CF_OPTIONS_START, log_entry.get_msg())
        if not parts or len(parts) != 1:
            return None
        # In case of match, we return the column-family name
        return parts[0]

    @staticmethod
    def is_cf_options_start_entry(log_entry):
        result = LogFileOptionsParser.try_parsing_as_cf_options_start_entry(
            log_entry)
        return result is not None

    @staticmethod
    def parse_db_wide_wbm_sub_pseudo_options(entry, options_dict):
        wbm_pseudo_options = \
            re.findall(regexes.DB_WIDE_WBM_PSEUDO_OPTION_LINE,
                       entry.get_msg(), re.MULTILINE)
        for pseudo_option_name, pseudo_option_value in wbm_pseudo_options:
            options_dict[f"write_buffer_manager_{pseudo_option_name}"] =\
                pseudo_option_value

    @staticmethod
    def parse_db_wide_options(log_entries, start_entry_idx, end_entry_idx):
        """
        Parses all of the entries in the specified range of
        [start_entry_idx, end_entry_idx)

        Returns:
            options_dict: The parsed options:
                dict(<option name>: <option value>)
            entry_idx: the index of the entry
        """
        logging.debug(f"Parsing DB-Wide Options ("
                      f"{format_lines_range_from_entries_idxs(log_entries,start_entry_idx, end_entry_idx)})") # noqa

        options_dict = {}
        entry_idx = start_entry_idx
        while entry_idx < end_entry_idx:
            entry = log_entries[entry_idx]
            options_kv = \
                LogFileOptionsParser.try_parsing_as_options_entry(entry)
            if options_kv:
                option_name, option_value = options_kv
                options_dict[option_name] = option_value
                if option_name == \
                    utils.\
                        DB_WIDE_WRITE_BUFFER_MANAGER_OPTIONS_NAME:
                    # Special case write buffer manager "Options"
                    LogFileOptionsParser.parse_db_wide_wbm_sub_pseudo_options(
                        entry, options_dict)
            else:
                # TODO - Add info to Error
                logging.error(f"TODO - ERROR In DB Wide Entry "
                              f"(idx:{entry_idx}), {entry}")

            entry_idx += 1

        return options_dict

    @staticmethod
    def parse_cf_options(log_entries, start_entry_idx, cf_name=None):
        logging.debug(
            f"Parsing CF Options ("
            f"{format_line_num_from_entry(log_entries[start_entry_idx])}")

        entry_idx = start_entry_idx

        # If cf_name was specified, it means we have received cf options
        # without the CF options header entry
        if cf_name is None:
            cf_name = LogFileOptionsParser. \
                try_parsing_as_cf_options_start_entry(log_entries[entry_idx])
            entry_idx += 1

        # cf_name may be the emtpy string, but not None
        assert cf_name is not None

        options_dict = {}
        table_options_dict = None
        duplicate_option = False
        while entry_idx < len(log_entries):
            entry = log_entries[entry_idx]
            options_kv = \
                LogFileOptionsParser.try_parsing_as_options_entry(entry)
            if options_kv:
                option_name, option_value = options_kv
                if option_name in options_dict:
                    # finding the same option twice implies that the options
                    # for this cf are over.
                    duplicate_option = True
                    break
                options_dict[option_name] = option_value
            else:
                temp_table_options_dict = \
                    LogFileOptionsParser.try_parsing_as_table_options_entry(
                           entry)
                if temp_table_options_dict:
                    assert table_options_dict is None
                    table_options_dict = temp_table_options_dict
                else:
                    # The entry is a different type of entry => done
                    break

            entry_idx += 1

        assert options_dict, "No Options for Column Family"
        assert table_options_dict, "Missing table options in CF options"

        logging.debug(f"Completed Parsing CF Options ([{cf_name}] "
                      f"{format_line_num_from_entry(log_entries[entry_idx])}, "
                      f"duplicate:{duplicate_option})")

        return cf_name, options_dict, table_options_dict, entry_idx, \
            duplicate_option
