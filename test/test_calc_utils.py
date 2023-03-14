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

from dataclasses import dataclass

import calc_utils
import db_options as db_opts
import test.testing_utils as test_utils
import utils
from counters import CountersMngr
from stats_mngr import CfFileHistogramStatsMngr
from test.sample_log_info import SampleLogInfo


# TODO: Move to the stats mngr test file

def test_get_cf_size_bytes():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)
    compactions_stats_mngr = \
        parsed_log.get_stats_mngr().get_compactions_stats_mngr()
    for cf_name in SampleLogInfo.CF_NAMES:
        actual_size_bytes = \
            compactions_stats_mngr.get_cf_size_bytes_at_end(cf_name)
        assert actual_size_bytes == SampleLogInfo.CF_SIZE_BYTES[cf_name]


def test_get_db_size_bytes():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)
    compactions_stats_mngr = \
        parsed_log.get_stats_mngr().get_compactions_stats_mngr()

    for level, level_size_bytes in \
            SampleLogInfo.CF_DEFAULT_LEVEL_SIZES.items():
        actual_level_size_bytes = \
            compactions_stats_mngr.get_cf_level_size_bytes(
                SampleLogInfo.CF_DEFAULT, level)
        assert actual_level_size_bytes == level_size_bytes


def test_calc_all_events_histogram():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)

    events_histogram = calc_utils.calc_all_events_histogram(
        SampleLogInfo.CF_NAMES, parsed_log.get_events_mngr())
    assert events_histogram == SampleLogInfo.EVENTS_HISTOGRAM


def test_calc_read_latency_per_cf_stats():
    time = "2023/01/04-09:04:59.378877 27442"
    l0 = 0
    l1 = 1
    l4 = 4
    cf1 = "cf1"
    cf2 = "cf2"

    stats_lines_cf1 = \
    f'''** File Read Latency Histogram By Level [{cf1}] **
    ** Level {l0} read latency histogram (micros):
    Count: 100 Average: 1.5  StdDev: 34.39
    Min: 0  Median: 2.4427  Max: 1000
    Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02
    
    ** Level {l4} read latency histogram (micros):
    Count: 200 Average: 2.5  StdDev: 2.2
    Min: 1000  Median: 3.3  Max: 2000
    Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02'''  # noqa
    stats_lines_cf1 = stats_lines_cf1.splitlines()
    stats_lines_cf1 = [line.strip() for line in stats_lines_cf1]

    mngr = CfFileHistogramStatsMngr()

    mngr.add_lines(time, cf1, stats_lines_cf1)
    total_reads_cf1 = 100 + 200
    expected_cf1_avg_read_latency_us = (100*1.5 + 200*2.5) / total_reads_cf1
    expected_cf1_stats = \
        calc_utils.CfReadLatencyStats(
            num_reads=total_reads_cf1,
            avg_read_latency_us=expected_cf1_avg_read_latency_us,
            max_read_latency_us=2000,
            read_percent_of_all_cfs=100.0)

    actual_stats = calc_utils.calc_read_latency_per_cf_stats(mngr)
    assert actual_stats == {cf1: expected_cf1_stats}

    stats_lines_cf2 = \
        f'''** File Read Latency Histogram By Level [{cf2}] **
    ** Level {l1} read latency histogram (micros):
    Count: 1000 Average: 2.5  StdDev: 34.39
    Min: 0  Median: 2.4427  Max: 20000
    Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02

    ** Level {l4} read latency histogram (micros):
    Count: 500 Average: 1.1  StdDev: 2.2
    Min: 1000  Median: 3.3  Max: 10000
    Percentiles: P50: 2.44 P75: 3.52 P99: 5.93 P99.9: 7.64 P99.99: 8.02''' # noqa
    stats_lines_cf2 = stats_lines_cf2.splitlines()
    stats_lines_cf2 = [line.strip() for line in stats_lines_cf2]

    mngr.add_lines(time, cf2, stats_lines_cf2)
    total_reads = 300 + 1500
    total_reads_cf2 = 1000 + 500
    expected_cf1_stats.read_percent_of_all_cfs = \
        (total_reads_cf1 / total_reads) * 100
    expected_cf2_read_percent_of_all_cfs = \
        (total_reads_cf2 / total_reads) * 100
    expected_cf2_avg_read_latency_us = (1000*2.5 + 500*1.1) / (1000+500)
    expected_cf2_stats = \
        calc_utils.CfReadLatencyStats(
            num_reads=total_reads_cf2,
            avg_read_latency_us=expected_cf2_avg_read_latency_us,
            max_read_latency_us=20000,
            read_percent_of_all_cfs=expected_cf2_read_percent_of_all_cfs)

    actual_stats = calc_utils.calc_read_latency_per_cf_stats(mngr)
    assert actual_stats == {cf1: expected_cf1_stats,
                            cf2: expected_cf2_stats}


@dataclass
class HistogramInfo:
    count: int = 0
    sum: int = 0

    def get_average(self):
        assert self.sum > 0
        return self.sum / self.count


@dataclass
class SeekInfo:
    time: str
    num_seeks: int = 0
    num_found: int = 0
    num_next: int = 0
    num_prev: int = 0
    seek_micros: HistogramInfo = HistogramInfo()


def get_seek_counter_and_histogram_entry(test_seek_info):
    assert isinstance(test_seek_info, SeekInfo)

    prefix = 'rocksdb.number.db'
    lines = list()
    lines.append(
        f'{test_seek_info.time} 100 [db/db_impl/db_impl.cc:753] STATISTICS:')
    lines.append(f'{prefix}.seek COUNT : {test_seek_info.num_seeks}')
    lines.append(f'{prefix}.seek.found COUNT : {test_seek_info.num_found}')
    lines.append(f'{prefix}.next COUNT : {test_seek_info.num_next}')
    lines.append(f'{prefix}.prev COUNT : {test_seek_info.num_prev}')
    lines.append(
        f'rocksdb.db.seek.micros P50 : 0.0 P95 : 0.0 P99 : 0.0 P100 : 0.0 '
        f'COUNT : {test_seek_info.seek_micros.count} '
        f'SUM : {test_seek_info.seek_micros.sum}')
    return test_utils.lines_to_entries(lines)


def test_get_applicable_seek_stats():
    get_seek_stats = calc_utils.get_applicable_seek_stats

    start_time = "2022/06/16-15:36:02.993900"
    start_plus_1_second = utils.get_time_relative_to(start_time, num_seconds=1)
    start_plus_10_seconds = \
        utils.get_time_relative_to(start_time, num_seconds=10)
    start_plus_20_seconds = \
        utils.get_time_relative_to(start_time, num_seconds=20)

    start_seek_stats = SeekInfo(start_time)
    start_entry = get_seek_counter_and_histogram_entry(start_seek_stats)

    start_plus_1_seek_stats = SeekInfo(start_plus_1_second)
    start_plus_1_seek_entry = \
        get_seek_counter_and_histogram_entry(start_plus_1_seek_stats)

    start_plus_10_seek_stats = \
        SeekInfo(start_plus_10_seconds, num_seeks=1000, num_found=500)
    start_plus_10_seek_entry = \
        get_seek_counter_and_histogram_entry(start_plus_10_seek_stats)

    start_plus_20_seek_stats = \
        SeekInfo(start_plus_20_seconds,
                 num_seeks=20000,
                 num_found=10000,
                 num_next=1500,
                 num_prev=500,
                 seek_micros=HistogramInfo(count=20000, sum=2000))
    start_plus_20_seek_entry = \
        get_seek_counter_and_histogram_entry(start_plus_20_seek_stats)

    mngr = CountersMngr()
    assert get_seek_stats(mngr) is None

    assert mngr.try_adding_entries(start_entry, 0) == (True, 1)
    assert mngr.get_last_counter_entry('rocksdb.number.db.seek') == \
           {'time': start_time, 'value': 0}
    assert get_seek_stats(mngr) is None

    assert mngr.try_adding_entries(start_plus_1_seek_entry, 0) == (True, 1)
    assert mngr.get_last_counter_entry('rocksdb.number.db.seek') == \
           {'time': start_plus_1_second, 'value': 0}
    assert get_seek_stats(mngr) is None

    assert mngr.try_adding_entries(start_plus_10_seek_entry, 0) == (True, 1)
    assert mngr.get_last_counter_entry('rocksdb.number.db.seek') == \
           {'time': start_plus_10_seconds, 'value': 1000}
    get_seek_stats(mngr) == calc_utils.SeekStats(num_seeks=1000,
                                                 num_found_seeks=500)

    assert mngr.try_adding_entries(start_plus_20_seek_entry, 0) == (True, 1)
    assert mngr.get_last_counter_entry('rocksdb.number.db.seek') == \
           {'time': start_plus_20_seconds, 'value': 20000}
    assert mngr.get_last_histogram_entry('rocksdb.db.seek.micros',
                                         non_zero=False) == \
           {'time': start_plus_20_seconds,
            'values': {'Average': 0.1,
                       'Count': 20000,
                       'Interval Count': 20000,
                       'Interval Sum': 2000,
                       'P100': 0.0,
                       'P50': 0.0,
                       'P95': 0.0,
                       'P99': 0.0,
                       'Sum': 2000}}
    assert get_seek_stats(mngr) == \
           calc_utils.SeekStats(num_seeks=20000,
                                num_found_seeks=10000,
                                num_nexts=1500,
                                num_prevs=500,
                                avg_seek_range_size=0.1,
                                avg_seek_rate_per_second=(2000/20/10**6),
                                avg_seek_latency_us=0.1)


CF_SECTION_TYPE = db_opts.SectionType.CF
TABLE_SECTION_TYPE = db_opts.SectionType.TABLE_OPTIONS


def test_get_cfs_common_and_specific_options():
    get_opts = calc_utils.get_cfs_common_and_specific_options

    cf1 = 'cf1'
    cf2 = 'cf2'

    cf_option1 = "CF-Option1"
    cf_option2 = "CF-Option2"
    cf_option3 = "CF-Option3"

    cf_table_option1 = "CF-Table-Option1"
    cf_table_option2 = "CF-Table-Option2"

    full_cf_option1_name = db_opts.get_full_option_name(CF_SECTION_TYPE,
                                                        cf_option1)
    full_cf_option2_name = db_opts.get_full_option_name(CF_SECTION_TYPE,
                                                        cf_option2)
    full_cf_option3_name = db_opts.get_full_option_name(CF_SECTION_TYPE,
                                                        cf_option3)
    full_cf_table_option1_name =\
        db_opts.get_full_option_name(TABLE_SECTION_TYPE, cf_table_option1)
    full_cf_table_option2_name = \
        db_opts.get_full_option_name(TABLE_SECTION_TYPE, cf_table_option2)

    dbo = db_opts.DatabaseOptions()
    assert get_opts(dbo) == ({}, {})

    dbo.set_cf_option(cf1, cf_option1, 1, allow_new_option=True)
    expected_common = {full_cf_option1_name: 1}
    expected_specific = {cf1: {}}
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf1, cf_option2, 2, allow_new_option=True)
    expected_common = {
        full_cf_option1_name: 1,
        full_cf_option2_name: 2
    }
    expected_specific = {cf1: {}}
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf2, cf_option3, 3, allow_new_option=True)
    expected_common = {}
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2},
        cf2: {full_cf_option3_name: 3}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf2, cf_option1, 1, allow_new_option=True)
    expected_common = {full_cf_option1_name: 1}
    expected_specific = {
        cf1: {full_cf_option2_name: 2},
        cf2: {full_cf_option3_name: 3}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf2, cf_option1, 5)
    expected_common = {}
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2},
        cf2: {full_cf_option1_name: 5,
              full_cf_option3_name: 3}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_table_option(cf2, cf_table_option1, 100, allow_new_option=True)
    expected_common = {}
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2},
        cf2: {full_cf_option1_name: 5,
              full_cf_option3_name: 3,
              full_cf_table_option1_name: 100}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_table_option(cf1, cf_table_option1, 100, allow_new_option=True)
    expected_common = {full_cf_table_option1_name: 100}
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2},
        cf2: {full_cf_option1_name: 5,
              full_cf_option3_name: 3}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_table_option(cf1, cf_table_option2, 200, allow_new_option=True)
    expected_common = {full_cf_table_option1_name: 100}
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2,
              full_cf_table_option2_name: 200},
        cf2: {full_cf_option1_name: 5,
              full_cf_option3_name: 3}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_table_option(cf2, cf_table_option2, 200, allow_new_option=True)
    expected_common = {
        full_cf_table_option1_name: 100,
        full_cf_table_option2_name: 200
    }
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2},
        cf2: {full_cf_option1_name: 5,
              full_cf_option3_name: 3}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf1, cf_option3, 3, allow_new_option=True)
    expected_common = {
        full_cf_option3_name: 3,
        full_cf_table_option1_name: 100,
        full_cf_table_option2_name: 200
    }
    expected_specific = {
        cf1: {full_cf_option1_name: 1,
              full_cf_option2_name: 2},
        cf2: {full_cf_option1_name: 5}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf1, cf_option1, 5)
    expected_common = {
        full_cf_option1_name: 5,
        full_cf_option3_name: 3,
        full_cf_table_option1_name: 100,
        full_cf_table_option2_name: 200
    }
    expected_specific = {
        cf1: {full_cf_option2_name: 2},
        cf2: {}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)

    dbo.set_cf_option(cf2, cf_option2, 2, allow_new_option=True)
    expected_common = {
        full_cf_option1_name: 5,
        full_cf_option2_name: 2,
        full_cf_option3_name: 3,
        full_cf_table_option1_name: 100,
        full_cf_table_option2_name: 200
    }
    expected_specific = {
        cf1: {},
        cf2: {}
    }
    assert get_opts(dbo) == (expected_common, expected_specific)


def test_get_cfs_common_and_specific_diff_dicts():
    get_cfs_diff = calc_utils.get_cfs_common_and_specific_diff_dicts

    # db_cf = utils.NO_CF
    dflt_cf = utils.DEFAULT_CF_NAME
    cf1 = 'cf1'
    # cf2 = 'cf2'

    db_option1 = "DB-Options1"
    db_option2 = "DB-Options2"

    cf_option1 = "CF-Option1"
    cf_option2 = "CF-Option2"
    # cf_option3 = "CF-Option3"
    # cf_table_option1 = "CF-Table-Option1"
    # cf_table_option2 = "CF-Table-Option2"

    full_cf_option1_name = db_opts.get_full_option_name(CF_SECTION_TYPE,
                                                        cf_option1)

    def extract_dict(cfs_options_diff):
        assert isinstance(cfs_options_diff, db_opts.CfsOptionsDiff)
        return cfs_options_diff.get_diff_dict(exclude_compared_cfs_names=True)

    base_dbo = db_opts.DatabaseOptions()
    log_dbo = db_opts.DatabaseOptions()
    assert get_cfs_diff(base_dbo, log_dbo) == ({}, {})

    base_dbo.set_db_wide_option(db_option1, 1, allow_new_option=True)
    assert get_cfs_diff(base_dbo, log_dbo) == ({}, {})

    log_dbo.set_db_wide_option(db_option2, 1, allow_new_option=True)
    assert get_cfs_diff(base_dbo, log_dbo) == ({}, {})

    # base has a cf option but log has no cf-s => empty diff
    base_dbo.set_cf_option(dflt_cf, cf_option1, 10, allow_new_option=True)
    assert get_cfs_diff(base_dbo, log_dbo) == ({}, {})

    base_dbo.set_cf_option(dflt_cf, cf_option1, 10, allow_new_option=True)
    log_dbo.set_cf_option(dflt_cf, cf_option1, 10, allow_new_option=True)
    assert get_cfs_diff(base_dbo, log_dbo) == ({}, {dflt_cf: None})

    base_dbo.set_cf_option(dflt_cf, cf_option1, 11, allow_new_option=True)
    log_dbo.set_cf_option(dflt_cf, cf_option1, 10, allow_new_option=True)
    actual_common, actual_specific = get_cfs_diff(base_dbo, log_dbo)
    assert actual_common.get_diff_dict(exclude_compared_cfs_names=True) == {
        full_cf_option1_name: (11, 10)}
    assert actual_specific == {dflt_cf: None}

    base_dbo.set_cf_option(dflt_cf, cf_option2, 20, allow_new_option=True)
    log_dbo.set_cf_option(dflt_cf, cf_option2, 20, allow_new_option=True)
    actual_common, actual_specific = get_cfs_diff(base_dbo, log_dbo)
    assert actual_common.get_diff_dict(exclude_compared_cfs_names=True) == {
        full_cf_option1_name: (11, 10)}
    assert actual_specific == {dflt_cf: None}

    log_dbo.set_cf_option(cf1, cf_option2, 20, allow_new_option=True)
    # base (dflt): opt1=11, opt2=20
    # log: dflt: opt1=10,  opt2=20
    #      cf1:  Missing   opt2=20
    # Expected diff: Common: None (opt2 identical)
    #                Specific: dflt: (11, 10), cf1: (11, Missing)
    actual_common, actual_specific = get_cfs_diff(base_dbo, log_dbo)
    actual_dflt = extract_dict(actual_specific[dflt_cf])
    actual_cf1 = extract_dict(actual_specific[cf1])
    assert actual_common == {}
    assert actual_dflt == {full_cf_option1_name: (11, 10)}
    assert actual_cf1 == {full_cf_option1_name: (11, "Missing")}

    log_dbo.set_cf_option(cf1, cf_option1, 11, allow_new_option=True)
    # base (dflt): opt1=11, opt2=20
    # log: dflt: opt1=10,  opt2=20
    #      cf1:  opt1=11   opt2=20
    # Expected diff: Common: None (opt2 identical)
    #                Specific: dflt: (11, 10), cf1: None
    actual_common, actual_specific = get_cfs_diff(base_dbo, log_dbo)
    actual_dflt = extract_dict(actual_specific[dflt_cf])
    assert actual_common == {}
    assert actual_dflt == {full_cf_option1_name: (11, 10)}
    assert actual_specific[cf1] is None
