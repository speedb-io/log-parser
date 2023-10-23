import copy
from dataclasses import dataclass

import db_files
import events
import utils
from test.testing_utils import create_event

job_id1 = 1
job_id2 = 2

cf1 = "cf1"
cf2 = "cf2"
cf_names = [cf1, cf2]

time1_minus_10_sec = "2023/01/24-08:54:30.130553"
time1 = "2023/01/24-08:54:40.130553"
time1_plus_10_sec = "2023/01/24-08:54:50.130553"
time1_plus_11_sec = "2023/01/24-08:55:50.130553"

file_number1 = 1234
file_number2 = 5678
file_number3 = 9999


@dataclass
class GlobalTestVars:
    event_time_micros: int = 0

    cmprsd_data_size_bytes: int = 62396458
    num_data_blocks: int = 1000
    total_keys_sizes_bytes: int = 2000
    total_values_sizes_bytes: int = 3333
    index_size: int = 3000
    filter_size: int = 4000
    num_entries: int = 5555
    filter_policy: str = "bloomfilter"
    num_filter_entries: int = 6666
    compression_type: str = "NoCompression"


def get_table_properties(global_vars):
    assert isinstance(global_vars, GlobalTestVars)

    return {
        "data_size": global_vars.cmprsd_data_size_bytes,
        "index_size": global_vars.index_size,
        "index_partitions": 0,
        "top_level_index_size": 0,
        "index_key_is_user_key": 1,
        "index_value_is_delta_encoded": 1,
        "filter_size": global_vars.filter_size,
        "raw_key_size": global_vars.total_keys_sizes_bytes,
        "raw_average_key_size": 24,
        "raw_value_size": global_vars.total_values_sizes_bytes,
        "raw_average_value_size": 1000,
        "num_data_blocks": global_vars.num_data_blocks,
        "num_entries": global_vars.num_entries,
        "num_filter_entries": global_vars.num_filter_entries,
        "num_deletions": 0,
        "num_merge_operands": 0,
        "num_range_deletions": 0,
        "format_version": 0,
        "fixed_key_len": 0,
        "filter_policy": global_vars.filter_policy,
        "column_family_name": "default",
        "column_family_id": 0,
        "comparator": "leveldb.BytewiseComparator",
        "merge_operator": "nullptr",
        "prefix_extractor_name": "nullptr",
        "property_collectors": "[]",
        "compression": global_vars.compression_type,
        "oldest_key_time": 1672823099,
        "file_creation_time": 1672823099,
        "slow_compression_estimated_data_size": 0,
        "fast_compression_estimated_data_size": 0,
        "db_id": "c100448c-dc04-4c74-8ab2-65d72f3aa3a8",
        "db_session_id": "4GAWIG5RIF8PQWM3NOQG",
        "orig_file_number": 37155}


def test_create_delete_file():
    vars = GlobalTestVars()

    creation_event1 = create_event(job_id1, cf_names, time1,
                                   events.EventType.TABLE_FILE_CREATION, cf1,
                                   file_number=file_number1,
                                   table_properties=get_table_properties(vars))

    monitor = db_files.DbFilesMonitor()
    assert monitor.get_all_live_files() == {}
    assert monitor.get_cf_live_files(cf1) == []

    expected_data_size_bytes = \
        vars.total_keys_sizes_bytes + vars.total_values_sizes_bytes

    info = \
        db_files.DbFileInfo(
            file_number=file_number1,
            cf_name=cf1,
            creation_time=time1,
            deletion_time=None,
            size_bytes=0,
            compressed_size_bytes=0,
            compressed_data_size_bytes=vars.cmprsd_data_size_bytes,
            data_size_bytes=expected_data_size_bytes,
            index_size_bytes=vars.index_size,
            filter_size_bytes=vars.filter_size,
            filter_policy=vars.filter_policy,
            num_filter_entries=vars.num_filter_entries,
            compression_type=vars.compression_type,
            level=None,
            creation_event=None,
            deletion_event=None)

    assert monitor.new_event(creation_event1)
    info1 = copy.deepcopy(info)
    info1.creation_event = creation_event1
    assert monitor.get_all_files() == {cf1: [info1]}
    assert monitor.get_all_cf_files(cf1) == [info1]
    assert monitor.get_all_cf_files(cf2) == []
    assert monitor.get_all_live_files() == {cf1: [info1]}
    assert monitor.get_cf_live_files(cf1) == [info1]
    assert monitor.get_cf_live_files(cf2) == []

    deletion_event = create_event(job_id1, cf_names, time1_plus_10_sec,
                                  events.EventType.TABLE_FILE_DELETION, cf1,
                                  file_number=file_number1)
    assert monitor.new_event(deletion_event)
    info1.deletion_time = time1_plus_10_sec
    info1.deletion_event = deletion_event
    assert monitor.get_all_files() == {cf1: [info1]}
    assert monitor.get_all_cf_files(cf1) == [info1]
    assert monitor.get_all_cf_files(cf2) == []
    assert monitor.get_all_live_files() == {}
    assert monitor.get_cf_live_files(cf1) == []
    assert monitor.get_cf_live_files(cf2) == []

    creation_event2 = create_event(job_id1, cf_names, time1_plus_10_sec,
                                   events.EventType.TABLE_FILE_CREATION, cf1,
                                   file_number=file_number2,
                                   table_properties=get_table_properties(vars))
    info2 = copy.deepcopy(info)
    info2.file_number = file_number2
    info2.creation_time = time1_plus_10_sec
    info2.creation_event = creation_event2

    assert monitor.new_event(creation_event2)
    assert monitor.get_all_files() == {cf1: [info1, info2]}
    assert monitor.get_all_cf_files(cf1) == [info1, info2]
    assert monitor.get_all_cf_files(cf2) == []
    assert monitor.get_all_live_files() == {cf1: [info2]}
    assert monitor.get_cf_live_files(cf2) == []
    assert monitor.get_cf_live_files(cf1) == [info2]

    creation_event3 = create_event(job_id1, cf_names, time1_plus_10_sec,
                                   events.EventType.TABLE_FILE_CREATION, cf2,
                                   file_number=file_number3,
                                   table_properties=get_table_properties(vars))
    info3 = copy.deepcopy(info)
    info3.file_number = file_number3
    info3.cf_name = cf2
    info3.creation_time = time1_plus_10_sec
    info3.creation_event = creation_event3

    assert monitor.new_event(creation_event3)
    assert monitor.get_all_files() == {cf1: [info1, info2], cf2: [info3]}
    assert monitor.get_all_cf_files(cf1) == [info1, info2]
    assert monitor.get_all_cf_files(cf2) == [info3]
    assert monitor.get_all_live_files() == {cf1: [info2], cf2: [info3]}
    assert monitor.get_cf_live_files(cf1) == [info2]
    assert monitor.get_cf_live_files(cf2) == [info3]


create_job_id = 100
delete_job_id = 200
elapsed_seconds = 0
created_file_number = 1


def test_live_file_stats():
    @dataclass
    class CreateTestVars:
        job_id: int = None
        file_number: int = None
        time: str = None
        cf: str = None
        index_size: int = None
        filter_size: int = None

        def __init__(self, cf, index_size, filter_size):
            global create_job_id, elapsed_seconds, created_file_number

            create_job_id += 1
            self.job_id = create_job_id

            created_file_number += 1
            self.file_number = created_file_number

            self.cf = cf
            self.index_size = index_size
            self.filter_size = filter_size

            global elapsed_seconds
            elapsed_seconds += 1
            self.time = utils.get_time_relative_to(time1, elapsed_seconds)

    @dataclass
    class DeleteTestVars:
        job_id: int = None
        file_number: int = None
        time: str = None
        cf: str = None

        def __init__(self, create_vars):
            assert isinstance(create_vars, CreateTestVars)

            global delete_job_id, elapsed_seconds

            delete_job_id += 1
            self.job_id = delete_job_id

            self.file_number = create_vars.file_number
            self.cf = create_vars.cf

            elapsed_seconds += 1
            self.time = utils.get_time_relative_to(time1, elapsed_seconds)

    def create_test_file(monitor, vars, global_vars):
        assert isinstance(vars, CreateTestVars)

        global_vars.index_size = vars.index_size
        global_vars.filter_size = vars.filter_size

        creation_event = \
            create_event(vars.job_id, cf_names, vars.time,
                         events.EventType.TABLE_FILE_CREATION, vars.cf,
                         file_number=vars.file_number,
                         table_properties=get_table_properties(global_vars))
        assert monitor.new_event(creation_event)
        return creation_event

    def delete_test_file(monitor, vars):
        assert isinstance(vars, DeleteTestVars)

        deletion_event = create_event(vars.job_id, cf_names, vars.time,
                                      events.EventType.TABLE_FILE_DELETION,
                                      vars.cf,
                                      file_number=vars.file_number)
        assert monitor.new_event(deletion_event)

    global_vars = GlobalTestVars()

    cf1_create_1_vars = \
        CreateTestVars(cf=cf1, index_size=100, filter_size=200)
    cf1_create_2_vars = \
        CreateTestVars(cf=cf1, index_size=50, filter_size=500)
    cf1_create_3_vars = \
        CreateTestVars(cf=cf1, index_size=170, filter_size=30)

    cf1_delete_1_vars = DeleteTestVars(cf1_create_1_vars)
    cf1_delete_2_vars = DeleteTestVars(cf1_create_2_vars)

    cf2_create_1_vars = \
        CreateTestVars(cf=cf2, index_size=100, filter_size=200)
    cf2_create_2_vars = \
        CreateTestVars(cf=cf2, index_size=400, filter_size=200)

    cf2_delete_1_vars = \
        DeleteTestVars(cf2_create_1_vars)

    monitor = db_files.DbFilesMonitor()

    create_test_file(monitor, cf1_create_1_vars, global_vars)
    # Live - cf1: index=100, filter=200

    delete_test_file(monitor, cf1_delete_1_vars)
    # Live: - cf1: index=0, filter=0

    cf1_create_2_event =\
        create_test_file(monitor, cf1_create_2_vars, global_vars)
    cf1_create_2_time = cf1_create_2_event.get_log_time()
    # Live: - cf1: index=50, filter=500

    cf2_create_1_event =\
        create_test_file(monitor, cf2_create_1_vars, global_vars)
    cf2_create_1_time = cf2_create_1_event.get_log_time()
    # Live: - cf1: index=50, filter=500, cf2: index=100, filter=200

    cf1_create_3_event = \
        create_test_file(monitor, cf1_create_3_vars, global_vars)
    cf1_create_3_time = cf1_create_3_event.get_log_time()
    # Live: - cf1: index=220, filter=530, cf2: index=100, filter=200

    delete_test_file(monitor, cf2_delete_1_vars)
    # Live: - cf1: index=220, filter=530, cf2: index=0, filter=0

    delete_test_file(monitor, cf1_delete_2_vars)
    # Live: - cf1: index=170, filter=30, cf2: index=0, filter=0

    cf2_create_2_event =\
        create_test_file(monitor, cf2_create_2_vars, global_vars)
    cf2_create_2_time = cf2_create_2_event.get_log_time()
    # Live: - cf1: index=170, filter=30, cf2: index=400, filter=200

    expected_cf1_index_stats = db_files.BlockLiveFileStats()
    expected_cf1_index_stats.num_created = 3
    expected_cf1_index_stats.num_live = 1
    expected_cf1_index_stats.total_created_size_bytes = 320
    expected_cf1_index_stats.curr_total_live_size_bytes = 170
    expected_cf1_index_stats.max_size_bytes = 170
    expected_cf1_index_stats.max_size_time = cf1_create_3_time
    expected_cf1_index_stats.max_total_live_size_bytes = 220
    expected_cf1_index_stats.max_total_live_size_time = cf1_create_3_time

    expected_cf1_filter_stats = db_files.BlockLiveFileStats()
    expected_cf1_filter_stats.num_created = 3
    expected_cf1_filter_stats.num_live = 1
    expected_cf1_filter_stats.total_created_size_bytes = 730
    expected_cf1_filter_stats.curr_total_live_size_bytes = 30
    expected_cf1_filter_stats.max_size_bytes = 500
    expected_cf1_filter_stats.max_size_time = cf1_create_2_time
    expected_cf1_filter_stats.max_total_live_size_bytes = 530
    expected_cf1_filter_stats.max_total_live_size_time = cf1_create_3_time

    expected_cf2_index_stats = db_files.BlockLiveFileStats()
    expected_cf2_index_stats.num_created = 2
    expected_cf2_index_stats.num_live = 1
    expected_cf2_index_stats.total_created_size_bytes = 500
    expected_cf2_index_stats.curr_total_live_size_bytes = 400
    expected_cf2_index_stats.max_size_bytes = 400
    expected_cf2_index_stats.max_size_time = cf2_create_2_time
    expected_cf2_index_stats.max_total_live_size_bytes = 400
    expected_cf2_index_stats.max_total_live_size_time = cf2_create_2_time

    expected_cf2_filter_stats = db_files.BlockLiveFileStats()
    expected_cf2_filter_stats.num_created = 2
    expected_cf2_filter_stats.num_live = 1
    expected_cf2_filter_stats.total_created_size_bytes = 400
    expected_cf2_filter_stats.curr_total_live_size_bytes = 200
    expected_cf2_filter_stats.max_size_bytes = 200
    expected_cf2_filter_stats.max_size_time = cf2_create_1_time
    expected_cf2_filter_stats.max_total_live_size_bytes = 200
    expected_cf2_filter_stats.max_total_live_size_time = cf2_create_1_time

    actual_stats = monitor.get_blocks_stats()
    assert actual_stats[cf1][db_files.BlockType.INDEX] == \
           expected_cf1_index_stats
    assert actual_stats[cf1][db_files.BlockType.FILTER] == \
           expected_cf1_filter_stats

    assert actual_stats[cf2][db_files.BlockType.INDEX] == \
           expected_cf2_index_stats
    assert actual_stats[cf2][db_files.BlockType.FILTER] == \
           expected_cf2_filter_stats

    expected_index_stats = db_files.BlockLiveFileStats()
    expected_index_stats.num_created = 5
    expected_index_stats.num_live = 2
    expected_index_stats.total_created_size_bytes = 820
    expected_index_stats.curr_total_live_size_bytes = 570
    expected_index_stats.max_size_bytes = 400
    expected_index_stats.max_size_time = cf2_create_2_time
    # These are incorrect for more than 1 cf
    expected_index_stats.max_total_live_size_bytes = 0
    expected_index_stats.max_total_live_size_time = None

    actual_index_stats = \
        db_files.get_block_stats_for_cfs_group(
            [cf1, cf2], monitor, db_files.BlockType.INDEX)
    assert actual_index_stats == expected_index_stats

    expected_filter_stats = db_files.BlockLiveFileStats()
    expected_filter_stats.num_created = 5
    expected_filter_stats.num_live = 2
    expected_filter_stats.total_created_size_bytes = 1130
    expected_filter_stats.curr_total_live_size_bytes = 230
    expected_filter_stats.max_size_bytes = 500
    expected_filter_stats.max_size_time = cf1_create_2_time
    # These are incorrect for more than 1 cf
    # expected_filter_stats.max_total_live_size_bytes = 530
    # expected_filter_stats.max_total_live_size_time = cf1_create_3_time

    actual_filter_stats = \
        db_files.get_block_stats_for_cfs_group(
            [cf1, cf2], monitor, db_files.BlockType.FILTER)
    assert actual_filter_stats == expected_filter_stats
