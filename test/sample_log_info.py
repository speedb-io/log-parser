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

from datetime import timedelta

import utils


class SampleLogInfo:
    FILE_PATH = "input_files/LOG_sample"
    START_TIME = "2022/04/17-14:13:10.723796"
    END_TIME = "2022/04/17-14:14:32.645120"
    PRODUCT_NAME = "SpeeDB"
    GIT_HASH = "UNKNOWN:0a396684d6c08f6fe4a37572c0429d91176c51d1"
    VERSION = "6.22.1"
    NUM_ENTRIES = 73
    CF_DEFAULT = 'default'
    CF_NAMES = [CF_DEFAULT, '_sample/CF_1', '_sample/CF-2', '']
    DB_WIDE_OPTIONS_START_ENTRY_IDX = 7
    SUPPORT_INFO_START_ENTRY_IDX = 15

    CF_DEFAULT_LEVEL_SIZES = {
        0: utils.get_num_bytes_from_human_readable_components("149.99", "MB"),
        1: utils.get_num_bytes_from_human_readable_components("271.79", "MB"),
        2: utils.get_num_bytes_from_human_readable_components("2.73", "GB"),
        3: utils.get_num_bytes_from_human_readable_components("24.96", "GB"),
        4: utils.get_num_bytes_from_human_readable_components("54.33", "GB"),
    }

    CF_SIZE_BYTES = \
        {
            'default':
                utils.get_num_bytes_from_human_readable_components(
                    "82.43", "GB"),
            '_sample/CF_1': None,
            '_sample/CF-2': None,
            '': None,
        }

    DB_SIZE_BYTES = sum([size if size is not None else 0 for size in
                         CF_SIZE_BYTES.values()])

    NUM_WARNS = 1

    OPTIONS_ENTRIES_INDICES = [21, 33, 42, 50]
    TABLE_OPTIONS_ENTRIES_INDICES = [24, 38, 45, 55]

    DB_WIDE_OPTIONS_DICT = {
        'error_if_exists': '0',
        'create_if_missing': '1',
        'db_log_dir': '',
        'wal_dir': '',
        'track_and_verify_wals_in_manifest': '0',
        'env': '0x7f4a9117d5c0',
        'fs': 'Posix File System',
        'info_log': '0x7f4af4020bf0'
    }

    DEFAULT_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator',
        'merge_operator': 'StringAppendTESTOperator',
        'write_buffer_size': '67108864',
        'max_write_buffer_number': '2',
        'ttl': '2592000'
    }

    SAMPLE_CF1_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator-XXX',
        'merge_operator': 'StringAppendTESTOperator-XXX',
        'compaction_filter': 'None',
        'table_factory': 'BlockBasedTable',
        'write_buffer_size': '67108864',
        'max_write_buffer_number': '2',
    }

    SAMPLE_CF2_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator-YYY',
        'table_factory': 'BlockBasedTable-YYY',
        'write_buffer_size': '123467108864',
        'max_write_buffer_number': '10',
        'compression': 'Snappy'
    }

    EMPTY_CF_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator-ZZZ',
        'merge_operator': 'StringAppendTESTOperator-ZZZ',
        'compaction_filter': 'None',
        'table_factory': 'BlockBasedTable'
    }

    DEFAULT_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af401f570)',
        'cache_index_and_filter_blocks': '1',
        'cache_index_and_filter_blocks_with_high_priority': '1',
        'pin_l0_filter_and_index_blocks_in_cache': '1',
        'pin_top_level_index_and_filter': '1',
        'metadata_cache_top_level_index_pinning': '0',
        'block_cache_capacity': '209715200',
        'block_cache_compressed': '(nil)',
        'prepopulate_block_cache': '0'}

    SAMPLE_CF1_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af4031090)',
        'pin_top_level_index_and_filter': '1',
        'metadata_cache_unpartitioned_pinning': '3',
        'no_block_cache': '0',
        'block_cache': '0x7f4bc07214d0',
        'block_cache_memory_allocator': 'None',
        'block_cache_high_pri_pool_ratio': '0.100',
        'block_cache_compressed': '(nil)'}

    SAMPLE_CF2_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af4091b90)',
        'cache_index_and_filter_blocks': '1',
        'cache_index_and_filter_blocks_with_high_priority': '1'
    }

    EMPTY_CF_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af4030f30)',
        'pin_top_level_index_and_filter': '1'}

    OPTIONS_DICTS = [
        DEFAULT_OPTIONS_DICT,
        SAMPLE_CF1_OPTIONS_DICT,
        SAMPLE_CF2_OPTIONS_DICT,
        EMPTY_CF_OPTIONS_DICT
    ]

    TABLE_OPTIONS_DICTS = [
        DEFAULT_TABLE_OPTIONS_DICT,
        SAMPLE_CF1_TABLE_OPTIONS_DICT,
        SAMPLE_CF2_TABLE_OPTIONS_DICT,
        EMPTY_CF_TABLE_OPTIONS_DICT
    ]

    DB_STATS_ENTRY_TIME = "2022/04/17-14:14:28.645150"
    CUMULATIVE_DURATION = \
        timedelta(hours=12, minutes=10, seconds=56, milliseconds=123)
    INTERVAL_DURATION = \
        timedelta(hours=45, minutes=34, seconds=12, milliseconds=789)
    DB_WIDE_STALLS_ENTRIES = \
        {DB_STATS_ENTRY_TIME: {"cumulative_duration": CUMULATIVE_DURATION,
                               "cumulative_percent": 98.7,
                               "interval_duration": INTERVAL_DURATION,
                               "interval_percent": 12.3}}

    EVENTS_HISTOGRAM = {'default': {"table_file_creation": 2},
                        '_sample/CF_1': {},
                        '_sample/CF-2': {},
                        '': {'flush_started': 1,
                             "table_file_creation": 1}}


class SampleRolledLogInfo:
    FILE_PATH = "input_files/Rolled_LOG_sample.txt"
    START_TIME = "2022/04/17-14:13:10.723796"
    END_TIME = "2022/04/17-14:14:32.645120"
    PRODUCT_NAME = "SpeeDB"
    GIT_HASH = "UNKNOWN:0a396684d6c08f6fe4a37572c0429d91176c51d1"
    VERSION = "6.22.1"
    NUM_ENTRIES = 59
    CF_NAMES = ["default", "", "CF1"]
    AUTO_GENERATED_CF_NAMES = ["Unknown-CF-#1",
                               "Unknown-CF-#2",
                               "Unknown-CF-#3"]
    DB_WIDE_OPTIONS_START_ENTRY_IDX = 7
    SUPPORT_INFO_START_ENTRY_IDX = 15

    NUM_WARNS = 1

    OPTIONS_ENTRIES_INDICES = [19, 25, 32, 38]
    TABLE_OPTIONS_ENTRIES_INDICES = [21, 29, 34, 42]

    DB_WIDE_OPTIONS_DICT = SampleLogInfo.DB_WIDE_OPTIONS_DICT
    DEFAULT_OPTIONS_DICT = SampleLogInfo.DEFAULT_OPTIONS_DICT
    SAMPLE_CF1_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF1_OPTIONS_DICT
    SAMPLE_CF2_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF2_OPTIONS_DICT
    EMPTY_CF_OPTIONS_DICT = SampleLogInfo.EMPTY_CF_OPTIONS_DICT
    DEFAULT_TABLE_OPTIONS_DICT = SampleLogInfo.DEFAULT_TABLE_OPTIONS_DICT
    SAMPLE_CF1_TABLE_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF1_TABLE_OPTIONS_DICT
    SAMPLE_CF2_TABLE_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF2_TABLE_OPTIONS_DICT
    EMPTY_CF_TABLE_OPTIONS_DICT = SampleLogInfo.EMPTY_CF_TABLE_OPTIONS_DICT

    OPTIONS_DICTS = [
        DEFAULT_OPTIONS_DICT,
        SAMPLE_CF1_OPTIONS_DICT,
        SAMPLE_CF2_OPTIONS_DICT,
        EMPTY_CF_OPTIONS_DICT
    ]

    TABLE_OPTIONS_DICTS = [
        DEFAULT_TABLE_OPTIONS_DICT,
        SAMPLE_CF1_TABLE_OPTIONS_DICT,
        SAMPLE_CF2_TABLE_OPTIONS_DICT,
        EMPTY_CF_TABLE_OPTIONS_DICT
    ]

    DB_STATS_ENTRY_TIME = "2022/04/17-14:14:28.645150"
    CUMULATIVE_DURATION = \
        timedelta(hours=12, minutes=10, seconds=56, milliseconds=123)
    INTERVAL_DURATION = \
        timedelta(hours=45, minutes=34, seconds=12, milliseconds=789)
    DB_WIDE_STALLS_ENTRIES = \
        {DB_STATS_ENTRY_TIME: {"cumulative_duration": CUMULATIVE_DURATION,
                               "cumulative_percent": 98.7,
                               "interval_duration": INTERVAL_DURATION,
                               "interval_percent": 12.3}}
