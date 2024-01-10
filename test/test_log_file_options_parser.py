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

from log_entry import LogEntry
from log_file_options_parser import get_table_options_topic_info, \
    LogFileOptionsParser
from test.sample_log_info import SampleLogInfo, SampleRolledLogInfo


def test_get_table_options_topic_info():
    assert get_table_options_topic_info(
        "metadata_cache_options") == ("metadata_cache_options",
                                      "metadata_cache_")
    assert get_table_options_topic_info(
        "block_cache_options") == ("block_cache_options", "block_cache_")

    assert get_table_options_topic_info(
        "pinning_policy_options") == ("pinning_policy_options",
                                      "pinning_policy_")

    assert get_table_options_topic_info("block_cache") is None


def read_sample_file(InfoClass):
    f = open(InfoClass.FILE_PATH)
    lines = f.readlines()
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

    assert len(entries) == InfoClass.NUM_ENTRIES

    return entries


def test_try_parsing_as_options_entry():
    date = "2022/04/17-14:13:10.725596 7f4a9fdff700"
    context = "7f4a9fdff700"
    line_start = date + " " + context + " "
    option1 = "Options.track_and_verify_wals_in_manifest: 0"
    option2 = "Options.wal_dir: /data/rocksdb2/"
    option3 = "Options.statistics: (nil)"
    option4 = "Options.comparator: leveldb.BytewiseComparator"
    table_file_option = "data_block_index_type: 0"

    assert ("track_and_verify_wals_in_manifest", "0") == \
           LogFileOptionsParser.try_parsing_as_options_entry(
               LogEntry(0, line_start + option1, True))
    assert ("track_and_verify_wals_in_manifest", "0") == \
           LogFileOptionsParser.try_parsing_as_options_entry(
               LogEntry(0, line_start + option1 + "   ", True))

    assert ("wal_dir", "/data/rocksdb2/") == \
           LogFileOptionsParser.try_parsing_as_options_entry(
               LogEntry(0, line_start + option2, True))
    assert ("statistics", "(nil)") == \
           LogFileOptionsParser.try_parsing_as_options_entry(
               LogEntry(0, line_start + option3, True))
    assert ("comparator", "leveldb.BytewiseComparator") == \
           LogFileOptionsParser.try_parsing_as_options_entry(
               LogEntry(0, line_start + option4, True))

    assert not LogFileOptionsParser.try_parsing_as_options_entry(
        LogEntry(0, line_start, True))
    assert not LogFileOptionsParser.try_parsing_as_options_entry(
        LogEntry(0, line_start + "   ", True))
    assert not LogFileOptionsParser.try_parsing_as_options_entry(
        LogEntry(0, line_start + "Options.xxx", True))
    assert not LogFileOptionsParser.try_parsing_as_options_entry(
        LogEntry(0, line_start + table_file_option, True))


def test_try_parsing_as_table_options_entry():
    date = "2022/04/17-14:13:10.725596"
    context = "7f4a9fdff700"
    line_start = date + " " + context + " "
    option1 = "Options.wal_dir: /data/rocksdb2/"
    table_options_start = "table_factory options:"
    table_options_line_start = line_start + table_options_start
    table_option1 = "flush_block_policy_factory: " \
                    "FlushBlockBySizePolicyFactory (0x7f4af4091b90)"
    table_option2 = "cache_index_and_filter_blocks: 1"
    table_option_special_value = " metadata_cache_options: "
    table_option_special_value_cont1 = "  partition_pinning: 0 "

    expected_result = dict()

    table_options_entry = LogEntry(0, table_options_line_start + " " +
                                   table_option1)
    expected_result["flush_block_policy_factory"] = \
        "FlushBlockBySizePolicyFactory (0x7f4af4091b90)"
    actual_result = LogFileOptionsParser.try_parsing_as_table_options_entry(
        table_options_entry)
    assert expected_result == actual_result

    table_options_entry.add_line(table_option2)
    expected_result["cache_index_and_filter_blocks"] = '1'
    actual_result = LogFileOptionsParser.try_parsing_as_table_options_entry(
        table_options_entry)
    assert expected_result == actual_result

    table_options_entry.add_line(table_option_special_value)
    actual_result = LogFileOptionsParser.try_parsing_as_table_options_entry(
        table_options_entry)
    assert expected_result == actual_result

    table_options_entry.add_line(table_option_special_value_cont1)
    expected_result["metadata_cache_partition_pinning"] = "0"
    actual_result = LogFileOptionsParser.try_parsing_as_table_options_entry(
        table_options_entry)
    assert actual_result == expected_result

    options_entry = LogEntry(0, line_start + option1, True)
    assert LogFileOptionsParser.try_parsing_as_options_entry(options_entry)
    assert not LogFileOptionsParser.try_parsing_as_table_options_entry(
        options_entry)


def test_parse_db_wide_options():
    log_entries = read_sample_file(SampleLogInfo)

    start_entry_idx = SampleLogInfo.DB_WIDE_OPTIONS_START_ENTRY_IDX
    actual_options_dict =\
        LogFileOptionsParser.parse_db_wide_options(
            log_entries,
            start_entry_idx,
            SampleLogInfo.SUPPORT_INFO_START_ENTRY_IDX)

    assert actual_options_dict == SampleLogInfo.DB_WIDE_OPTIONS_DICT


def test_parsing_as_table_options_entry():
    log_entries = read_sample_file(SampleLogInfo)

    for i, idx in enumerate(SampleLogInfo.TABLE_OPTIONS_ENTRIES_INDICES):
        actual_options_dict = \
            LogFileOptionsParser.try_parsing_as_table_options_entry(
                log_entries[idx])
        assert SampleLogInfo.TABLE_OPTIONS_DICTS[i] == actual_options_dict


def test_parse_cf_options_with_cf_options_header():
    log_entries = read_sample_file(SampleLogInfo)

    for i, idx in enumerate(SampleLogInfo.OPTIONS_ENTRIES_INDICES):
        cf_name, options_dict, table_options_dict, entry_idx, \
            duplicate_option =\
            LogFileOptionsParser.parse_cf_options(log_entries, idx)

        assert cf_name == SampleLogInfo.CF_NAMES[i]
        assert options_dict == SampleLogInfo.OPTIONS_DICTS[i]
        assert table_options_dict == SampleLogInfo.TABLE_OPTIONS_DICTS[i]
        assert not duplicate_option

        # +1 entry for the cf options start line (not a cf-options entry)
        # +1 for the table options entry (single entry)
        num_parsed_entries = 1 + len(options_dict) + 1
        assert entry_idx == idx + num_parsed_entries


def test_parse_cf_options_without_cf_options_header():
    log_entries = read_sample_file(SampleRolledLogInfo)

    for i, idx in enumerate(SampleRolledLogInfo.OPTIONS_ENTRIES_INDICES):
        provided_cf_name = SampleLogInfo.CF_NAMES[i]
        cf_name, options_dict, table_options_dict, entry_idx, \
            duplicate_option =\
            LogFileOptionsParser.parse_cf_options(log_entries, idx,
                                                  provided_cf_name)

        assert cf_name == provided_cf_name
        assert options_dict == SampleLogInfo.OPTIONS_DICTS[i]
        assert table_options_dict == SampleLogInfo.TABLE_OPTIONS_DICTS[i]
        # the last cf should NOT have a duplicate, the ones befire it should
        assert duplicate_option == (i+1 < len(
            SampleRolledLogInfo.OPTIONS_ENTRIES_INDICES))

        # +1 for the table options entry (single entry)
        num_parsed_entries = len(options_dict) + 1
        assert entry_idx == idx + num_parsed_entries
