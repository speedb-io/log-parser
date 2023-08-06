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
from stats_mngr import DbWideStatsMngr, CompactionStatsMngr, BlobStatsMngr, \
    CfFileHistogramStatsMngr, BlockCacheStatsMngr, \
    StatsMngr, parse_uptime_line, CfNoFileStatsMngr
from test.testing_utils import lines_to_entries

NUM_LINES = 381
STATS_DUMP_IDX = 0
DB_STATS_IDX = 2
COMPACTION_STATS_DEFAULT_PER_LEVEL_IDX = 11
COMPACTION_STATS_DEFAULT_BY_PRIORITY_IDX = 22
BLOB_STATS_IDX = 27
CF_NO_FILE_HISTOGRAM_DEFAULT_IDX = 29
BLOCK_CACHE_DEFAULT_IDX = 38
CF_FILE_READ_LATENCY_IDX = 41
STATISTICS_COUNTERS_IDX = 138
ONE_PAST_STATS_IDX = 377

EMPTY_LINE1 = ''
EMPTY_LINE2 = '                      '

get_value_by_size_with_unit = \
    utils.get_num_bytes_from_human_readable_str


# TODO  - Test that errors during parsing of a stats entry are handled
#  gracefully (parsing continues, partial/corrupt data is not saved, and only
#  the faulty lines are skipped)

'''
TODO:
General:
- Test parse_line_with_cf

DB-Wide:
Add tests for stalls lines or remove the code
'''


def read_sample_stats_file():
    f = open("input_files/LOG_sample_stats.txt")
    lines = f.readlines()
    assert len(lines) == NUM_LINES
    return lines


def test_parse_uptime_line():
    line1 = "Uptime(secs): 4.8 total, 8.4 interval"
    assert (4.8, 8.4) == parse_uptime_line(line1)

    line2 = "Uptime(secs): 4.8 total, XXX 8.4 interval"
    assert parse_uptime_line(line2, allow_mismatch=True) is None


#
#   DB Wide Stats Mngr
#

def test_db_wide_is_stats_start_line():
    assert DbWideStatsMngr.is_start_line("** DB Stats **")
    assert DbWideStatsMngr.is_start_line("** DB Stats **      ")
    assert not DbWideStatsMngr.is_start_line("** DB Stats **   DUMMY TEXT")
    assert not DbWideStatsMngr.is_start_line("** DB XXX Stats **")


def test_db_wide_try_parse_as_stalls_line():
    pass


def test_db_wide_stats_mngr():
    time1 = "2022/11/24-15:58:09.511260"
    db_wide_stats_lines1 = \
        '''Uptime(secs): 4.8 total, 4.8 interval
        Cumulative writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 GB, 0.00 MB/s
        Cumulative WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
        Cumulative stall: 12:10:56.123 H:M:S, 98.7 percent
        Interval writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 MB, 0.00 MB/s
        Interval WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
        Interval stall: 45:34:12.789 H:M:S, 12.3 percent
        '''.splitlines()  # noqa

    time2 = "2022/11/24-15:59:09.511260"
    db_wide_stats_lines2 = \
        '''Uptime(secs): 4.8 total, 4.8 interval
        Cumulative writes: 10M writes, 2M keys, 0 commit groups, 0.0 writes per commit group, ingest: 1.23 GB, 5.67 MB/s
        '''.splitlines()  # noqa

    mngr = DbWideStatsMngr()
    assert mngr.get_stalls_entries() == {}

    mngr.add_lines(time1, db_wide_stats_lines1)
    expected_cumulative_duration = \
        timedelta(hours=12, minutes=10, seconds=56, milliseconds=123)
    expected_interval_duration = \
        timedelta(hours=45, minutes=34, seconds=12, milliseconds=789)
    expected_stalls_entries = \
        {time1: {"cumulative_duration": expected_cumulative_duration,
                 "cumulative_percent": 98.7,
                 "interval_duration": expected_interval_duration,
                 "interval_percent": 12.3}}
    expected_cumulative_writes_entries = {
        time1: DbWideStatsMngr.CumulativeWritesInfo()
    }

    actual_stalls_entries = mngr.get_stalls_entries()
    assert actual_stalls_entries == expected_stalls_entries

    actual_cumulative_writes_entries = mngr.get_cumulative_writes_entries()
    assert actual_cumulative_writes_entries == \
           expected_cumulative_writes_entries

    mngr.add_lines(time2, db_wide_stats_lines2)
    actual_stalls_entries = mngr.get_stalls_entries()
    assert actual_stalls_entries == expected_stalls_entries

    expected_cumulative_writes_entries.update({
        time2: DbWideStatsMngr.CumulativeWritesInfo(
            num_writes=utils.get_number_from_human_readable_str("10 M"),
            num_keys=utils.get_number_from_human_readable_str("2 M"),
            ingest=utils.get_num_bytes_from_human_readable_str("1.23 GB"),
            ingest_rate_mbps=5.67)
    })

    actual_cumulative_writes_entries = mngr.get_cumulative_writes_entries()
    assert actual_cumulative_writes_entries == \
           expected_cumulative_writes_entries


def test_is_compaction_stats_start_line():
    line1 = "** Compaction Stats [default] **"
    assert CompactionStatsMngr.is_start_line(line1)
    assert CompactionStatsMngr.parse_start_line(line1) == "default"

    line2 = "** Compaction Stats [col-family] **       "
    assert CompactionStatsMngr.is_start_line(line2)
    assert CompactionStatsMngr.parse_start_line(line2) == "col-family"

    line3 = "       ** Compaction Stats [col-family] **"
    assert CompactionStatsMngr.is_start_line(line3)
    assert CompactionStatsMngr.parse_start_line(line3) == "col-family"

    line4 = "** Compaction Stats    [col-family]     **     "
    assert CompactionStatsMngr.is_start_line(line4)
    assert CompactionStatsMngr.parse_start_line(line4) == "col-family"

    line5 = "** Compaction XXX Stats  [col-family] **"
    assert not CompactionStatsMngr.is_start_line(line5)


def test_is_blob_stats_start_line():
    line1 = \
        'Blob file count: 0, total size: 0.0 GB, garbage size: 0.0 GB,' \
        ' space amp: 0.0'

    assert BlobStatsMngr.is_start_line(line1)
    assert (0, 0.0, 0.0, 0.0) == BlobStatsMngr.parse_blob_stats_line(line1)

    line2 = \
        'Blob file count: 100, total size: 1.5 GB, garbage size: 3.5 GB,' \
        ' space amp: 0.2'
    assert BlobStatsMngr.is_start_line(line2)
    assert (100, 1.5, 3.5, 0.2) == BlobStatsMngr.parse_blob_stats_line(line2)


def test_is_cf_file_histogram_stats_start_line():
    cf1 = "default"
    cf2 = "col_family"

    line1 = f"** File Read Latency Histogram By Level [{cf1}] **"
    assert CfFileHistogramStatsMngr.is_start_line(line1)
    assert CfFileHistogramStatsMngr.parse_start_line(line1) == cf1

    line2 = f"** File Read Latency Histogram By Level [{cf2}] **       "
    assert CfFileHistogramStatsMngr.is_start_line(line2)
    assert CfFileHistogramStatsMngr.parse_start_line(line2) == cf2

    line3 = f"       ** File Read Latency Histogram By Level [{cf2}] **"
    assert CfFileHistogramStatsMngr.is_start_line(line3)
    assert CfFileHistogramStatsMngr.parse_start_line(line3) == cf2

    line4 = \
        f"** File Read Latency Histogram By Level    [{cf2}]     **     "
    assert CfFileHistogramStatsMngr.is_start_line(line4)
    assert CfFileHistogramStatsMngr.parse_start_line(line4) == cf2

    line5 = \
        f"** File Read Latency Histogram XXX By Level Stats  [{cf2}] **"
    assert not CfFileHistogramStatsMngr.is_start_line(line5)


def test_is_block_cache_stats_start_line():
    line1 = 'Block cache LRUCache@0x5600bb634770#32819 capacity: 8.00 MB ' \
            'collections: 1 last_copies: 0 last_secs: 4.9e-05 secs_since: 0'

    line2 = \
        'Block cache entry stats(count,size,portion): ' \
        'Misc(3,8.12 KB, 0.0991821%)'

    assert BlockCacheStatsMngr.is_start_line(line1)
    assert not BlockCacheStatsMngr.is_start_line(line2)


def test_find_next_start_line_in_db_stats():
    lines = read_sample_stats_file()
    assert DbWideStatsMngr.is_start_line(lines[DB_STATS_IDX])

    expected_next_line_idxs = [COMPACTION_STATS_DEFAULT_PER_LEVEL_IDX,
                               COMPACTION_STATS_DEFAULT_BY_PRIORITY_IDX,
                               BLOB_STATS_IDX,
                               CF_NO_FILE_HISTOGRAM_DEFAULT_IDX,
                               BLOCK_CACHE_DEFAULT_IDX,
                               CF_FILE_READ_LATENCY_IDX]
    expected_next_types = [StatsMngr.StatsType.COMPACTION,
                           StatsMngr.StatsType.COMPACTION,
                           StatsMngr.StatsType.BLOB,
                           StatsMngr.StatsType.CF_NO_FILE,
                           StatsMngr.StatsType.BLOCK_CACHE,
                           StatsMngr.StatsType.CF_FILE_HISTOGRAM]

    expected_next_cf_names = ["default", "default", None, None,
                              None, "CF1"]

    line_idx = DB_STATS_IDX
    stats_type = StatsMngr.StatsType.DB_WIDE
    for i, expected_next_line_idx in enumerate(expected_next_line_idxs):
        next_line_idx, next_stats_type, next_cf_name = \
            StatsMngr.find_next_start_line_in_db_stats(lines,
                                                       line_idx,
                                                       stats_type)
        assert (next_line_idx > line_idx) or (next_stats_type is None)
        assert next_line_idx == expected_next_line_idx
        assert next_stats_type == expected_next_types[i]
        assert next_cf_name == expected_next_cf_names[i]

        line_idx = next_line_idx
        stats_type = next_stats_type


def test_blob_stats_mngr():
    blob_line = \
        'Blob file count: 10, total size: 1.5 GB, garbage size: 2.0 GB, ' \
        'space amp: 4.0'
    blob_lines = [blob_line, EMPTY_LINE2]
    time = '2022/11/24-15:58:09.511260 32851'
    cf = "cf1"
    mngr = BlobStatsMngr()
    mngr.add_lines(time, cf, blob_lines)

    expected_blob_entries = \
        {time: {"File Count": 10,
                "Total Size": float(1.5 * 2 ** 30),
                "Garbage Size": float(2 * 2 ** 30),
                "Space Amp": 4.0}}

    assert mngr.get_cf_stats(cf) == expected_blob_entries


def test_block_cache_stats_mngr_no_cf():
    lines = \
        '''Block cache LRUCache@0x5600bb634770#32819 capacity: 8.00 GB collections: 1 last_copies: 0 last_secs: 4.9e-05 secs_since: 0
        Block cache entry stats(count,size,portion): DataBlock(1548,6.97 MB,0.136142%) IndexBlock(843,3.91 GB,78.2314%) Misc(6,16.37 KB,1.86265e-08%)
        '''.splitlines()  # noqa

    time = '2022/11/24-15:58:09.511260'
    cf_name = "cf1"
    mngr = BlockCacheStatsMngr()
    cache_id = mngr.add_lines(time, cf_name, lines)
    assert cache_id == "LRUCache@0x5600bb634770#32819"

    entries = mngr.get_cache_entries(cache_id)
    assert entries['Capacity'] == get_value_by_size_with_unit('8.00 GB')

    expected_data_block_stats = \
        {'Count': 1548,
         'Size': get_value_by_size_with_unit('6.97 MB'),
         'Portion': '0.14%'
         }
    expected_index_block_stats = \
        {'Count': 843,
         'Size': get_value_by_size_with_unit('3.91 GB'),
         'Portion': '78.23%'
         }

    assert time in entries
    time_entries = entries[time]
    assert 'DataBlock' in time_entries
    assert time_entries['DataBlock'] == expected_data_block_stats

    assert 'IndexBlock' in time_entries
    assert time_entries['IndexBlock'] == expected_index_block_stats

    total_expected_usage = \
        get_value_by_size_with_unit('6.97 MB') + \
        get_value_by_size_with_unit('3.91 GB') + \
        get_value_by_size_with_unit('16.37 KB')
    assert time_entries["Usage"] == total_expected_usage
    assert entries["Usage"] == total_expected_usage

    assert mngr.get_cf_cache_entries(cache_id, cf_name) == {}
    assert mngr.get_last_usage(cache_id) == total_expected_usage


def test_block_cache_stats_mngr_with_cf():
    lines = \
        '''Block cache LRUCache@0x5600bb634770#32819 capacity: 8.00 GB collections: 1 last_copies: 0 last_secs: 4.9e-05 secs_since: 0
        Block cache entry stats(count,size,portion): DataBlock(1548,6.97 MB,0.136142%) IndexBlock(843,3.91 GB,78.2314%) Misc(6,16.37 KB,1.86265e-08%)
        Block cache [CF1]  DataBlock(4.50 KB) FilterBlock(0.00 KB) IndexBlock(0.91 GB)
        '''.splitlines()  # noqa

    time = '2022/11/24-15:58:09.511260'
    cf_name = "CF1"
    mngr = BlockCacheStatsMngr()
    cache_id = mngr.add_lines(time, cf_name, lines)
    assert cache_id == "LRUCache@0x5600bb634770#32819"

    cf_entries = mngr.get_cf_cache_entries(cache_id, cf_name)
    expected_cf_entries = \
        {time: {'DataBlock': get_value_by_size_with_unit('4.50 KB'),
                'IndexBlock': get_value_by_size_with_unit('0.91 GB')}
         }
    assert cf_entries == expected_cf_entries

    entries = mngr.get_cache_entries(cache_id)
    total_expected_usage = \
        get_value_by_size_with_unit('6.97 MB') + \
        get_value_by_size_with_unit('3.91 GB') + \
        get_value_by_size_with_unit('16.37 KB')
    assert entries[time]["Usage"] == total_expected_usage
    assert entries["Usage"] == total_expected_usage


def test_stats_mngr():
    lines = read_sample_stats_file()
    entries = lines_to_entries(lines)

    mngr = StatsMngr()

    expected_entry_idx = 1
    expected_cfs_names_found = set()
    assert mngr.try_adding_entries(entries, start_entry_idx=1) == \
           (False, expected_entry_idx, expected_cfs_names_found)
    expected_entry_idx = 2
    expected_cfs_names_found = {"default", "CF1"}
    assert mngr.try_adding_entries(entries, start_entry_idx=0) == \
           (True, expected_entry_idx, expected_cfs_names_found)


def test_stats_mngr_non_contig_entries_1():
    lines = \
'''2023/07/18-19:27:01.889729 27127 [/db_impl/db_impl.cc:1084] ------- DUMPING STATS -------
2023/07/18-19:27:01.889745 26641 [/column_family.cc:1044] [default] Increasing compaction threads because of estimated pending compaction bytes 18555651178
2023/07/18-19:27:01.890259 27127 [/db_impl/db_impl.cc:1086] 
** DB Stats **
Uptime(secs): 0.7 total, 0.7 interval
Cumulative writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 GB, 0.00 MB/s
Cumulative WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
Cumulative stall: 00:00:0.000 H:M:S, 0.0 percent
Interval writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 MB, 0.00 MB/s
Interval WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
Interval stall: 00:00:0.000 H:M:S, 0.0 percent
Write Stall (count): write-buffer-manager-limit-stops: 0,
 ** Compaction Stats [default] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      2/0   322.40 MB   1.3      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    594.4      0.12              0.00         1    0.120       0      0       0.0       0.0
  L1      6/1   350.91 MB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
  L2     59/11   3.16 GB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
  L3    487/28  27.78 GB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
  L4    166/0   10.17 GB   0.0      0.0     0.0      0.0       0.0      0.0       0.2   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
 Sum    720/40  41.77 GB   0.0      0.0     0.0      0.0       0.1      0.1       0.2   1.0      0.0    594.4      0.12              0.00         1    0.120       0      0       0.0       0.0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.1      0.1       0.2   1.0      0.0    594.4      0.12              0.00         1    0.120       0      0       0.0       0.0
'''.splitlines() # noqa

    entries = lines_to_entries(lines)

    mngr = StatsMngr()

    expected_entry_idx = 1
    expected_cfs_names_found = set()
    assert mngr.try_adding_entries(entries, start_entry_idx=0) == \
           (True, expected_entry_idx, expected_cfs_names_found)

    assert mngr.try_adding_entries(entries, start_entry_idx=1) == \
           (False, expected_entry_idx, expected_cfs_names_found)

    expected_entry_idx = 3
    expected_cfs_names_found = {"default"}
    assert mngr.try_adding_entries(entries, start_entry_idx=2) == \
           (True, expected_entry_idx, expected_cfs_names_found)


def test_stats_mngr_non_contig_entries_2():
    lines = \
'''2023/07/18-19:27:01.889729 27127 [/db_impl/db_impl.cc:1084] ------- DUMPING STATS -------
2023/07/18-19:27:01.889745 26641 [/column_family.cc:1044] [default] Increasing compaction threads because of estimated pending compaction bytes 18555651178
2023/07/18-19:27:01.889806 26641 (Original Log Time 2023/07/18-19:27:01.887253) [/db_impl/db_impl_compaction_flush.cc:3428] [default] Moving #13947 to level-4 67519682 bytes
2023/07/18-19:27:01.889746 27127 [/db_impl/db_impl.cc:1084] ------- DUMPING STATS -------
2023/07/18-19:27:01.890259 27127 [/db_impl/db_impl.cc:1086] 
** DB Stats **
Uptime(secs): 0.7 total, 0.7 interval
Cumulative writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 GB, 0.00 MB/s
Cumulative WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
Cumulative stall: 00:00:0.000 H:M:S, 0.0 percent
Interval writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 MB, 0.00 MB/s
Interval WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
Interval stall: 00:00:0.000 H:M:S, 0.0 percent
Write Stall (count): write-buffer-manager-limit-stops: 0,
 ** Compaction Stats [default] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      2/0   322.40 MB   1.3      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    594.4      0.12              0.00         1    0.120       0      0       0.0       0.0
  L1      6/1   350.91 MB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
  L2     59/11   3.16 GB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
  L3    487/28  27.78 GB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
  L4    166/0   10.17 GB   0.0      0.0     0.0      0.0       0.0      0.0       0.2   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
 Sum    720/40  41.77 GB   0.0      0.0     0.0      0.0       0.1      0.1       0.2   1.0      0.0    594.4      0.12              0.00         1    0.120       0      0       0.0       0.0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.1      0.1       0.2   1.0      0.0    594.4      0.12              0.00         1    0.120       0      0       0.0       0.0
'''.splitlines() # noqa

    entries = lines_to_entries(lines)

    mngr = StatsMngr()

    expected_entry_idx = 1
    expected_cfs_names_found = set()
    assert mngr.try_adding_entries(entries, start_entry_idx=0) == \
           (True, expected_entry_idx, expected_cfs_names_found)

    expected_entry_idx = 1
    expected_cfs_names_found = set()
    assert mngr.try_adding_entries(entries, start_entry_idx=1) == \
           (False, expected_entry_idx, expected_cfs_names_found)

    expected_entry_idx = 2
    expected_cfs_names_found = set()
    assert mngr.try_adding_entries(entries, start_entry_idx=2) == \
           (False, expected_entry_idx, expected_cfs_names_found)

    expected_entry_idx = 5
    expected_cfs_names_found = {"default"}
    assert mngr.try_adding_entries(entries, start_entry_idx=3) == \
           (True, expected_entry_idx, expected_cfs_names_found)


def test_compaction_stats_mngr():
    lines_level = \
        '''** Compaction Stats [default] **
        Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
        ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          L0      1/0   149.99 MB   0.6      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
          L1      5/0   271.79 MB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
          L2     50/1    2.73 GB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
          L3    421/6   24.96 GB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
          L4   1022/0   54.33 GB   0.2      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
         Sum   1499/7   82.43 GB   0.0      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
         Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
        '''.splitlines()  # noqa

    lines_priority = \
        '''** Compaction Stats [CF1] **
        Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.1      0.1       0.0   0.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
        '''.splitlines()  # noqa

    lines_level = [line.strip() for line in lines_level]
    lines_priority = [line.strip() for line in lines_priority]

    time = "2022/11/24-15:58:09.511260"
    mngr = CompactionStatsMngr()

    assert mngr.get_cf_size_bytes_at_end("default") is None

    mngr.add_lines(time, "default", lines_level)
    mngr.add_lines(time, "CF1", lines_priority)

    assert mngr.get_cf_size_bytes_at_end("default") == \
           utils.get_num_bytes_from_human_readable_components("82.43", "GB")
    assert mngr.get_cf_size_bytes_at_end("CF1") is None


def test_cf_no_file_stats_mngr():
    time = "2022/11/24-15:58:09.511260"
    cf_name = "cf1"
    lines = '''
    Uptime(secs): 22939219.8 total, 0.0 interval
    Flush(GB): cumulative 158.813, interval 0.000
    AddFile(GB): cumulative 0.000, interval 0.000
    AddFile(Total Files): cumulative 0, interval 0
    AddFile(L0 Files): cumulative 0, interval 0
    AddFile(Keys): cumulative 0, interval 0
    Cumulative compaction: 364.52 GB write, 0.02 MB/s write, 363.16 GB read, 0.02 MB/s read, 1942.7 seconds
    Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
    Stalls(count): 0 level0_slowdown, 1 level0_slowdown_with_compaction, 2 level0_numfiles, 3 level0_numfiles_with_compaction, 4 stop for pending_compaction_bytes, 5 slowdown for pending_compaction_bytes, 6 memtable_compaction, 7 memtable_slowdown, interval 100 total count
    '''.splitlines()  # noqa

    expected_stall_counts = \
        {cf_name: {time: {'level0_slowdown': 0,
                          'level0_slowdown_with_compaction': 1,
                          'level0_numfiles': 2,
                          'level0_numfiles_with_compaction': 3,
                          'stop for pending_compaction_bytes': 4,
                          'slowdown for pending_compaction_bytes': 5,
                          'memtable_compaction': 6,
                          'memtable_slowdown': 7,
                          'interval_total_count': 100}}}
    mngr = CfNoFileStatsMngr()
    mngr.add_lines(time, cf_name, lines)
    stall_counts = mngr.get_stall_counts()

    assert stall_counts == expected_stall_counts


def test_compaction_stats_get_field_value():
    stats = \
        {'2023/04/09-12:37:27.398344':
         {'LEVEL-0': {'Moved(GB)': '1.2',
                      'W-Amp': '1.0',
                      'Comp(sec)': '74.4',
                      'CompMergeCPU(sec)': '10.1',
                      'Comp(cnt)': '15.03'},
          'LEVEL-1': {'Moved(GB)': '1.2', 'W-Amp': '2.0',
                      'Comp(sec)': '84.4',
                      'CompMergeCPU(sec)': '20.2',
                      'Comp(cnt)': '15.03'},
          'SUM': {'Moved(GB)': '1.2',
                  'W-Amp': '3.0',
                  'Comp(sec)': '158.8',
                  'CompMergeCPU(sec)': '30.3',
                  'Comp(cnt)': '15.03'},
          'INTERVAL': {'Moved(GB)': '1.2',
                       'W-Amp': '1.5',
                       'Comp(sec)': '74.4',
                       'CompMergeCPU(sec)': '15.97',
                       'Comp(cnt)': '15.03'}}}

    field = CompactionStatsMngr.LevelFields.WRITE_AMP
    assert CompactionStatsMngr.get_level_field_value(stats, 0, field) == '1.0'
    assert CompactionStatsMngr.get_level_field_value(stats, 1, field) == '2.0'
    assert CompactionStatsMngr.get_sum_value(stats, field) == '3.0'

    assert CompactionStatsMngr.get_field_value_for_all_levels(stats, field) ==\
           {0: '1.0', 1: '2.0'}

    field = CompactionStatsMngr.LevelFields.COMP_SEC
    assert CompactionStatsMngr.get_level_field_value(stats, 0, field) == '74.4'
    assert CompactionStatsMngr.get_level_field_value(stats, 1, field) == '84.4'
    assert CompactionStatsMngr.get_sum_value(stats, field) == '158.8'

    assert CompactionStatsMngr.get_field_value_for_all_levels(stats, field) ==\
           {0: '74.4', 1: '84.4'}

    field = CompactionStatsMngr.LevelFields.COMP_MERGE_CPU
    assert CompactionStatsMngr.get_level_field_value(stats, 0, field) == '10.1'
    assert CompactionStatsMngr.get_level_field_value(stats, 1, field) == '20.2'
    assert CompactionStatsMngr.get_sum_value(stats, field) == '30.3'

    assert CompactionStatsMngr.get_field_value_for_all_levels(stats, field) ==\
           {0: '10.1', 1: '20.2'}


def test_compactions_stats_mngr():
    stats_lines = \
        f'''** Compaction Stats [default] **
    Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
      L0     16/11   3.90 GB   5.9    131.5     0.0    131.5     231.0     99.5       0.0   2.3    224.8    394.9    599.04            488.24      2319    0.258    136M   322K       0.0       0.0
      L1     30/30   1.81 GB   0.0    156.4    95.8     60.6     153.8     93.2       0.0   1.6    354.8    349.0    451.24            372.78        52    8.678    162M  2648K       0.0       0.0
      L2     43/0    2.47 GB   1.0    162.1    77.6     84.5     159.7     75.2      14.0   2.1    319.5    314.8    519.66            404.59      1075    0.483    168M  2502K       0.0       0.0
      L3    436/0   24.96 GB   1.0    473.6    89.2    384.4     445.7     61.3       0.0   5.0    327.9    308.5   1479.17           1127.43      1470    1.006    492M    29M       0.0       0.0
      L4   1369/0   66.58 GB   0.3    169.1    61.3    107.9     129.6     21.8       0.0   2.1    362.4    277.7    477.96            344.39       996    0.480    175M    41M       0.0       0.0
     Sum   1894/41  99.72 GB   0.0   1092.7   323.9    768.8    1119.9    351.0      14.0  11.2    317.3    325.1   3527.07           2737.42      5912    0.597   1136M    75M       0.0       0.0
     Int      0/0    0.00 KB   0.0   1092.7   323.9    768.8    1119.9    351.0      14.0  11.2    317.3    325.1   3527.07           2737.42      5912    0.597   1136M    75M       0.0       0.0'''.splitlines() # noqa
    stats_lines = [line.strip() for line in stats_lines]

    expected_level0_dict = \
        {'Num-Files': '16',
         'Files-In-Comp': '11',
         'size_bytes': 4187593113,
         'Score': '5.9',
         'Read(GB)': '131.5',
         'Rn(GB)': '0.0',
         'Rnp1(GB)': '131.5',
         'Write(GB)': '231.0',
         'Wnew(GB)': '99.5',
         'Moved(GB)': '0.0',
         'W-Amp': '2.3',
         'Rd(MB/s)': '224.8',
         'Wr(MB/s)': '394.9',
         'Comp(sec)': '599.04',
         'CompMergeCPU(sec)': '488.24',
         'Comp(cnt)': '2319',
         'Avg(sec)': '0.258',
         'KeyIn': '136M',
         'KeyDrop': '322K',
         'Rblob(GB)': '0.0',
         'Wblob(GB)': '0.0'}

    expected_sum_dict = \
        {'Num-Files': '1894',
         'Files-In-Comp': '41',
         'size_bytes': 107073534689,
         'Score': '0.0',
         'Read(GB)': '1092.7',
         'Rn(GB)': '323.9',
         'Rnp1(GB)': '768.8',
         'Write(GB)': '1119.9',
         'Wnew(GB)': '351.0',
         'Moved(GB)': '14.0',
         'W-Amp': '11.2',
         'Rd(MB/s)': '317.3',
         'Wr(MB/s)': '325.1',
         'Comp(sec)': '3527.07',
         'CompMergeCPU(sec)': '2737.42',
         'Comp(cnt)': '5912',
         'Avg(sec)': '0.597',
         'KeyIn': '1136M',
         'KeyDrop': '75M',
         'Rblob(GB)': '0.0',
         'Wblob(GB)': '0.0'}

    time = "2023/01/04-09:04:59.378877 27442"
    cf_name = "default"
    level0_key = 'LEVEL-0'
    sum_key = "SUM"
    mngr = CompactionStatsMngr()
    mngr.add_lines(time, cf_name, stats_lines)
    entries = mngr.get_cf_level_entries(cf_name)
    assert isinstance(entries, list)
    assert len(entries) == 1
    assert isinstance(entries[0], dict)
    assert list(entries[0].keys()) == [time]
    values = entries[0][time]
    assert isinstance(values, dict)
    assert len(values.keys()) == 7
    assert level0_key in values
    level0_values_dict = values[level0_key]
    assert level0_values_dict == expected_level0_dict
    assert sum_key in values
    sum_values_dict = values[sum_key]
    assert sum_values_dict == expected_sum_dict

    field = CompactionStatsMngr.LevelFields.WRITE_AMP
    assert CompactionStatsMngr.get_level_field_value(entries[0], 1, field) ==\
           '1.6'
    assert CompactionStatsMngr.get_sum_value(entries[0], field) == '11.2'
    assert CompactionStatsMngr.get_field_value_for_all_levels(
        entries[0], field) == {0: '2.3',
                               1: '1.6',
                               2: '2.1',
                               3: '5.0',
                               4: '2.1'}

    field = CompactionStatsMngr.LevelFields.COMP_SEC
    assert CompactionStatsMngr.get_level_field_value(entries[0], 1, field) == \
           '451.24'
    assert CompactionStatsMngr.get_sum_value(entries[0], field) == '3527.07'

    field = CompactionStatsMngr.LevelFields.COMP_MERGE_CPU
    assert CompactionStatsMngr.get_level_field_value(entries[0], 1, field) == \
           '372.78'
    assert CompactionStatsMngr.get_sum_value(entries[0], field) == '2737.42'


#
# FILE READ LATENCY TESTS INFO
#
time1 = "2023/01/04-09:04:59.378877 27442"
time2 = "2023/01/04-09:14:59.378877 27442"
l0 = 0
l4 = 4
cf1 = "cf1"
cf2 = "cf2"

stats_lines_cf1_t1_l0 = \
f'''** File Read Latency Histogram By Level [{cf1}] **
** Level {l0} read latency histogram (micros):
Count: 26687513 Average: 3.1169  StdDev: 34.39
Min: 0  Median: 2.4427  Max: 56365
Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02''' # noqa
stats_lines_cf1_t1_l0 = stats_lines_cf1_t1_l0.splitlines()
stats_lines_cf1_t1_l0 = [line.strip() for line in stats_lines_cf1_t1_l0]

stats_lines_cf1_t1_l4 = \
f'''** File Read Latency Histogram By Level [{cf1}] **
** Level {l4} read latency histogram (micros):
Count: 100 Average: 1.1  StdDev: 2.2
Min: 1000  Median: 3.3  Max: 2000
Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02''' # noqa
stats_lines_cf1_t1_l4 = stats_lines_cf1_t1_l4.splitlines()
stats_lines_cf1_t1_l4 = [line.strip() for line in stats_lines_cf1_t1_l4]

stats_lines_cf1_t2_l4 = \
f'''** File Read Latency Histogram By Level [{cf1}] **
** Level {l4} read latency histogram (micros):
Count: 500 Average: 10.10  StdDev: 20.20
Min: 10000  Median: 30.30  Max: 20000
Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02''' # noqa
stats_lines_cf1_t2_l4 = stats_lines_cf1_t2_l4.splitlines()
stats_lines_cf1_t2_l4 = [line.strip() for line in stats_lines_cf1_t2_l4]

stats_lines_cf2_t1_l0 = \
f'''** File Read Latency Histogram By Level [{cf2}] **
** Level {l0} read latency histogram (micros):
Count: 200 Average: 6.6  StdDev: 7.7
Min: 5000  Median: 8.8  Max: 6000
Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02''' # noqa
stats_lines_cf2_t1_l0 = stats_lines_cf2_t1_l0.splitlines()
stats_lines_cf2_t1_l0 = [line.strip() for line in stats_lines_cf2_t1_l0]

expected_cf1_t1_l0_stats = \
    CfFileHistogramStatsMngr.CfLevelStats(
        count=26687513,
        average=3.1169,
        std_dev=34.39,
        min=0,
        median=2.4427,
        max=56365)

expected_cf1_t1_l4_stats = \
    CfFileHistogramStatsMngr.CfLevelStats(
        count=100,
        average=1.1,
        std_dev=2.2,
        min=1000,
        median=3.3,
        max=2000)

expected_cf1_t2_l4_stats = \
    CfFileHistogramStatsMngr.CfLevelStats(
        count=500,
        average=10.10,
        std_dev=20.20,
        min=10000,
        median=30.30,
        max=20000)

expected_cf2_t1_l0_stats = \
    CfFileHistogramStatsMngr.CfLevelStats(
        count=200,
        average=6.6,
        std_dev=7.7,
        min=5000,
        median=8.8,
        max=6000)


def test_cf_file_histogram_mngr1():
    mngr = CfFileHistogramStatsMngr()

    expected_cf1_entries = dict()

    mngr.add_lines(time1, cf1, stats_lines_cf1_t1_l0)
    expected_cf1_entries[time1] = {l0:  expected_cf1_t1_l0_stats}
    assert mngr.get_cf_entries(cf1) == expected_cf1_entries
    assert mngr.get_last_cf_entry(cf1) == expected_cf1_entries
    assert mngr.get_cf_entries(cf2) is None

    mngr.add_lines(time1, cf1, stats_lines_cf1_t1_l4)
    expected_cf1_entries[time1][l4] = expected_cf1_t1_l4_stats
    assert mngr.get_cf_entries(cf1) == expected_cf1_entries
    assert mngr.get_last_cf_entry(cf1) == expected_cf1_entries
    assert mngr.get_cf_entries(cf2) is None

    expected_cf2_entries = dict()
    expected_cf2_entries[time1] = {}
    expected_cf2_entries[time1][l0] = expected_cf2_t1_l0_stats

    mngr.add_lines(time1, cf2, stats_lines_cf2_t1_l0)
    assert mngr.get_cf_entries(cf2) == expected_cf2_entries
    assert mngr.get_last_cf_entry(cf2) == expected_cf2_entries

    expected_cf1_entries[time2] = {}
    expected_cf1_entries[time2][l4] = expected_cf1_t2_l4_stats

    mngr.add_lines(time2, cf1, stats_lines_cf1_t2_l4)
    assert mngr.get_cf_entries(cf1) == expected_cf1_entries
    assert mngr.get_last_cf_entry(cf1) == \
           {time2: {l4: expected_cf1_t2_l4_stats}}


def test_cf_file_histogram_mngr2():
    empty_line = "                      "
    all_cf1_stats_lines = list()
    all_cf1_stats_lines.extend(stats_lines_cf1_t1_l0)
    all_cf1_stats_lines.extend([empty_line] * 2)
    all_cf1_stats_lines.extend(stats_lines_cf1_t1_l4[1:])
    all_cf1_stats_lines.extend([empty_line])

    mngr = CfFileHistogramStatsMngr()

    expected_cf1_entries = {
        time1: {l0: expected_cf1_t1_l0_stats,
                l4: expected_cf1_t1_l4_stats}}

    mngr.add_lines(time1, cf1, all_cf1_stats_lines)
    assert mngr.get_cf_entries(cf1) == expected_cf1_entries


stats_dump = """2022/10/07-16:50:52.365328 7f68d1999700 [db/db_impl/db_impl.cc:901] ------- DUMPING STATS -------
2022/10/07-16:50:52.365535 7f68d1999700 [db/db_impl/db_impl.cc:903] 
** DB Stats **
Uptime(secs): 4.1 total, 4.1 interval
Cumulative writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 GB, 0.00 MB/s
Cumulative WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 GB, 0.00 MB/s
Cumulative stall: 00:00:0.000 H:M:S, 0.0 percent
Interval writes: 0 writes, 0 keys, 0 commit groups, 0.0 writes per commit group, ingest: 0.00 MB, 0.00 MB/s
Interval WAL: 0 writes, 0 syncs, 0.00 writes per sync, written: 0.00 MB, 0.00 MB/s
Interval stall: 00:00:0.000 H:M:S, 0.0 percent

** Compaction Stats [default] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      5/0   154.26 MB   1.2      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0     11.7      0.32              0.00         1    0.315       0      0
 Sum   1127/0   70.96 GB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0     11.7      0.32              0.00         1    0.315       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0     11.7      0.32              0.00         1    0.315       0      0

** Compaction Stats [default] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0     11.7      0.32              0.00         1    0.315       0      0
Uptime(secs): 4.1 total, 4.1 interval
Flush(GB): cumulative 0.004, interval 0.004
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.90 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.3 seconds
Interval compaction: 0.00 GB write, 0.90 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.3 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [default] **
** Level 0 read latency histogram (micros):
Count: 25 Average: 1571.7200  StdDev: 5194.93
Min: 1  Median: 2.8333  Max: 26097
Percentiles: P50: 2.83 P75: 32.50 P99: 26097.00 P99.9: 26097.00 P99.99: 26097.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####


** Compaction Stats [meta-Kdm3W] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      2/0    1.95 KB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.1      0.01              0.00         1    0.007       0      0
  L1      1/0    1.04 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0
 Sum      3/0    2.99 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.1      0.01              0.00         1    0.007       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.1      0.01              0.00         1    0.007       0      0

** Compaction Stats [meta-Kdm3W] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.1      0.01              0.00         1    0.007       0      0
Uptime(secs): 4.1 total, 4.1 interval
Flush(GB): cumulative 0.000, interval 0.000
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [meta-Kdm3W] **
** Level 0 read latency histogram (micros):
Count: 8 Average: 3.0000  StdDev: 3.20
Min: 1  Median: 1.0000  Max: 11
Percentiles: P50: 1.00 P75: 3.00 P99: 11.00 P99.9: 11.00 P99.99: 11.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####

** Level 1 read latency histogram (micros):
Count: 4 Average: 2.5000  StdDev: 0.87
Min: 2  Median: 2.0000  Max: 4
Percentiles: P50: 2.00 P75: 2.00 P99: 3.96 P99.9: 4.00 P99.99: 4.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####


** Compaction Stats [txlog-Kdm3W] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Sum      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0

** Compaction Stats [txlog-Kdm3W] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Uptime(secs): 4.1 total, 4.1 interval
Flush(GB): cumulative 0.000, interval 0.000
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [txlog-Kdm3W] **

** Compaction Stats [!qs-stats-nvbl9e] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      2/0    1.88 KB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.4      0.00              0.00         1    0.003       0      0
  L1      1/0    1.10 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0
 Sum      3/0    2.97 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.4      0.00              0.00         1    0.003       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.4      0.00              0.00         1    0.003       0      0

** Compaction Stats [!qs-stats-nvbl9e] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.4      0.00              0.00         1    0.003       0      0
Uptime(secs): 4.1 total, 4.1 interval
Flush(GB): cumulative 0.000, interval 0.000
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [!qs-stats-nvbl9e] **
** Level 0 read latency histogram (micros):
Count: 8 Average: 2.1250  StdDev: 1.27
Min: 1  Median: 1.3333  Max: 5
Percentiles: P50: 1.33 P75: 2.00 P99: 5.00 P99.9: 5.00 P99.99: 5.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####


** Compaction Stats [default] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      5/0   154.26 MB   1.2      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0     11.7      0.32              0.00         1    0.315       0      0
 Sum   1127/0   70.96 GB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0     11.7      0.32              0.00         1    0.315       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0

** Compaction Stats [default] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0     11.7      0.32              0.00         1    0.315       0      0
Uptime(secs): 4.1 total, 0.0 interval
Flush(GB): cumulative 0.004, interval 0.000
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.90 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.3 seconds
Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [default] **
** Level 0 read latency histogram (micros):
Count: 25 Average: 1571.7200  StdDev: 5194.93
Min: 1  Median: 2.8333  Max: 26097
Percentiles: P50: 2.83 P75: 32.50 P99: 26097.00 P99.9: 26097.00 P99.99: 26097.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####

** Compaction Stats [meta-Kdm3W] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  L0      2/0    1.95 KB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.1      0.01              0.00         1    0.007       0      0
  L1      1/0    1.04 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0
 Sum      3/0    2.99 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   1.0      0.0      0.1      0.01              0.00         1    0.007       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0

** Compaction Stats [meta-Kdm3W] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.1      0.01              0.00         1    0.007       0      0
Uptime(secs): 4.1 total, 0.0 interval
Flush(GB): cumulative 0.000, interval 0.000
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [meta-Kdm3W] **
** Level 0 read latency histogram (micros):
Count: 8 Average: 3.0000  StdDev: 3.20
Min: 1  Median: 1.0000  Max: 11
Percentiles: P50: 1.00 P75: 3.00 P99: 11.00 P99.9: 11.00 P99.99: 11.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####

** Level 1 read latency histogram (micros):
Count: 4 Average: 2.5000  StdDev: 0.87
Min: 2  Median: 2.0000  Max: 4
Percentiles: P50: 2.00 P75: 2.00 P99: 3.96 P99.9: 4.00 P99.99: 4.00
------------------------------------------------------
(       1,       2 ]        3  75.000%  75.000% ###############
(       3,       4 ]        1  25.000% 100.000% #####


** Compaction Stats [txlog-Kdm3W] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Sum      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0
 Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0

** Compaction Stats [txlog-Kdm3W] **
Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Uptime(secs): 4.1 total, 0.0 interval
Flush(GB): cumulative 0.000, interval 0.000
AddFile(GB): cumulative 0.000, interval 0.000
AddFile(Total Files): cumulative 0, interval 0
AddFile(L0 Files): cumulative 0, interval 0
AddFile(Keys): cumulative 0, interval 0
Cumulative compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
Stalls(count): 0 level0_slowdown, 0 level0_slowdown_with_compaction, 0 level0_numfiles, 0 level0_numfiles_with_compaction, 0 stop for pending_compaction_bytes, 0 slowdown for pending_compaction_bytes, 0 memtable_compaction, 0 memtable_slowdown, interval 0 total count

** File Read Latency Histogram By Level [txlog-Kdm3W] **

 """ # noqa
stats_dump = stats_dump.splitlines()
stats_dump = [line.strip() for line in stats_dump]
stats_dump_entries = lines_to_entries(stats_dump)


def test_stats_mngr_parsing_ignoring_repeating_cfs():
    mngr = StatsMngr()
    assert mngr.is_dump_stats_start(stats_dump_entries[0])
    mngr.try_adding_entries(stats_dump_entries, 0)
