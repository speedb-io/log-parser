from dataclasses import dataclass

import db_files
from db_files import DbFilesMonitor
from events import EventType
from test.testing_utils import create_event


@dataclass
class FileCreationHelperInfo:
    job_id: int
    file_number: int
    cf_name: str
    creation_time: str
    compressed_data_size_bytes: int = 10000
    num_data_blocks: int = 500
    total_keys_sizes_bytes: int = 800
    total_values_sizes_bytes: int = 1600
    index_size: int = 1000
    filter_size: int = 2000
    num_entries: int = 100
    compression_type: str = "NoCompression"
    filter_policy: str = ""
    num_filter_entries: int = 50


def create_file_event_helper(cf_names, creation_info, monitor):
    assert isinstance(creation_info, FileCreationHelperInfo)

    table_properties = {
        "data_size": creation_info.compressed_data_size_bytes,
        "index_size": creation_info.index_size,
        "filter_size": creation_info.filter_size,
        "raw_key_size": creation_info.total_keys_sizes_bytes,
        "raw_value_size": creation_info.total_values_sizes_bytes,
        "num_data_blocks": creation_info.num_data_blocks,
        "num_entries": creation_info.num_entries,
        "compression": creation_info.compression_type,
        "filter_policy": creation_info.filter_policy,
        "num_filter_entries": creation_info.num_filter_entries}

    creation_event = create_event(creation_info.job_id, cf_names,
                                  creation_info.creation_time,
                                  EventType.TABLE_FILE_CREATION,
                                  creation_info.cf_name,
                                  file_number=creation_info.file_number,
                                  table_properties=table_properties)

    assert monitor.new_event(creation_event)


def test_calc_cf_files_stats():
    cf1 = "cf1"
    cf2 = "cf2"
    cf_names = [cf1, cf2]
    time1 = "2023/01/24-08:54:40.130553"
    time1_plus_10_sec = "2023/01/24-08:54:50.130553"
    time1_plus_11_sec = "2023/01/24-08:55:50.130553"

    monitor = DbFilesMonitor()

    helper_info1 = FileCreationHelperInfo(job_id=1,
                                          file_number=1234,
                                          cf_name=cf1,
                                          creation_time=time1,
                                          index_size=1000,
                                          filter_size=2000,
                                          filter_policy="Filter1",
                                          num_filter_entries=10000)

    # the block statistics are tested as part of the test_db_files suite
    expected_cf1_filter_specific_stats = \
        {cf1: db_files.CfFilterSpecificStats(filter_policy="Filter1",
                                             avg_bpk=8*2000/10000)}
    create_file_event_helper(cf_names, helper_info1, monitor)
    actual_cf1_stats = db_files.calc_cf_files_stats([cf1], monitor)
    assert actual_cf1_stats.cfs_filter_specific == \
           expected_cf1_filter_specific_stats
    assert db_files.calc_cf_files_stats([cf2], monitor) is None

    helper_info2 = FileCreationHelperInfo(job_id=2,
                                          file_number=5678,
                                          cf_name=cf1,
                                          creation_time=time1_plus_10_sec,
                                          index_size=500,
                                          filter_size=3000,
                                          filter_policy="Filter1",
                                          num_filter_entries=5000)
    expected_cf1_filter_specific_stats = \
        {cf1: db_files.CfFilterSpecificStats(filter_policy="Filter1",
                                             avg_bpk=8*5000/15000)}
    create_file_event_helper(cf_names, helper_info2, monitor)
    actual_cf1_stats = db_files.calc_cf_files_stats([cf1], monitor)
    assert actual_cf1_stats.cfs_filter_specific == \
           expected_cf1_filter_specific_stats
    assert db_files.calc_cf_files_stats([cf2], monitor) is None

    helper_info3 = FileCreationHelperInfo(job_id=3,
                                          file_number=9999,
                                          cf_name=cf2,
                                          creation_time=time1_plus_11_sec,
                                          index_size=1500,
                                          filter_size=1000,
                                          filter_policy="",
                                          num_filter_entries=0)
    expected_cf2_filter_specific_stats = \
        {cf2: db_files.CfFilterSpecificStats(filter_policy=None, avg_bpk=0)}
    expected_all_cfs_filter_specific_stats = {
        cf1: expected_cf1_filter_specific_stats[cf1],
        cf2: expected_cf2_filter_specific_stats[cf2]
    }

    create_file_event_helper(cf_names, helper_info3, monitor)
    actual_cf1_stats = db_files.calc_cf_files_stats([cf1], monitor)
    assert actual_cf1_stats.cfs_filter_specific == \
           expected_cf1_filter_specific_stats
    actual_cf2_stats = db_files.calc_cf_files_stats([cf2], monitor)
    assert actual_cf2_stats.cfs_filter_specific == \
           expected_cf2_filter_specific_stats
    actual_all_cfs_stats = db_files.calc_cf_files_stats([cf1, cf2], monitor)
    assert actual_all_cfs_stats.cfs_filter_specific == \
           expected_all_cfs_filter_specific_stats
