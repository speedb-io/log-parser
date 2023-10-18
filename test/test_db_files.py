import copy
from dataclasses import dataclass

import db_files
import events
import utils
import test.testing_utils as test_utils

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


def test_create_delete_file():
    table_vars = test_utils.TablePropertiesTestVars()

    creation_event1 = \
        test_utils.create_event(
            job_id1, cf_names, time1, events.EventType.TABLE_FILE_CREATION,
            cf1, file_number=file_number1,
            table_properties=test_utils.get_table_properties(table_vars))

    monitor = db_files.DbFilesMonitor()
    assert monitor.get_all_live_files() == {}
    assert monitor.get_cf_live_files(cf1) == []

    expected_data_size_bytes = \
        table_vars.total_keys_sizes_bytes + table_vars.total_values_sizes_bytes

    info = \
        db_files.DbFileInfo(
            file_number=file_number1,
            cf_name=cf1,
            creation_time=time1,
            deletion_time=None,
            compressed_file_size_bytes=0,
            compressed_data_size_bytes=0,
            data_size_bytes=expected_data_size_bytes,
            index_size_bytes=table_vars.index_size_bytes,
            filter_size_bytes=table_vars.filter_size_bytes,
            filter_policy=table_vars.filter_policy,
            num_filter_entries=table_vars.num_filter_entries,
            compression_type=table_vars.compression_type,
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

    deletion_event = \
        test_utils.create_event(job_id1, cf_names, time1_plus_10_sec,
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

    creation_event2 = \
        test_utils.create_event(
            job_id1, cf_names, time1_plus_10_sec,
            events.EventType.TABLE_FILE_CREATION, cf1,
            file_number=file_number2,
            table_properties=test_utils.get_table_properties(table_vars))
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

    creation_event3 = \
        test_utils.create_event(
            job_id1, cf_names, time1_plus_10_sec,
            events.EventType.TABLE_FILE_CREATION, cf2,
            file_number=file_number3,
            table_properties=test_utils.get_table_properties(table_vars))
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


@dataclass
class LiveFilesCommonTestVars:
    create_job_id = 100
    delete_job_id = 200
    elapsed_seconds = 0
    created_file_number = 1


@dataclass
class CreateTestVars(LiveFilesCommonTestVars):
    job_id: int = None
    file_number: int = None
    time: str = None
    cf: str = None
    total_keys_sizes_bytes: int = None
    total_values_sizes_bytes: int = None
    index_size: int = None
    filter_size: int = None

    def __init__(self, cf, total_keys_sizes_bytes, total_values_sizes_bytes,
                 index_size, filter_size):
        LiveFilesCommonTestVars.create_job_id += 1
        self.job_id = LiveFilesCommonTestVars.create_job_id

        LiveFilesCommonTestVars.created_file_number += 1
        self.file_number = LiveFilesCommonTestVars.created_file_number

        self.cf = cf
        self.total_keys_sizes_bytes = total_keys_sizes_bytes
        self.total_values_sizes_bytes = total_values_sizes_bytes
        self.index_size = index_size
        self.filter_size = filter_size

        LiveFilesCommonTestVars.elapsed_seconds += 1
        self.time = \
            utils.get_time_relative_to(
                time1, LiveFilesCommonTestVars.elapsed_seconds)


@dataclass
class DeleteTestVars:
    job_id: int = None
    file_number: int = None
    time: str = None
    cf: str = None

    def __init__(self, create_vars):
        assert isinstance(create_vars, CreateTestVars)

        LiveFilesCommonTestVars.delete_job_id += 1
        self.job_id = LiveFilesCommonTestVars.delete_job_id

        self.file_number = create_vars.file_number
        self.cf = create_vars.cf

        LiveFilesCommonTestVars.elapsed_seconds += 1
        self.time = \
            utils.get_time_relative_to(
                time1, LiveFilesCommonTestVars.elapsed_seconds)


def create_test_file(monitor, table_vars, global_vars):
    assert isinstance(table_vars, CreateTestVars)
    assert isinstance(global_vars, test_utils.TablePropertiesTestVars)

    global_vars.total_keys_sizes_bytes = table_vars.total_keys_sizes_bytes
    global_vars.total_values_sizes_bytes = table_vars.total_values_sizes_bytes
    global_vars.index_size_bytes = table_vars.index_size
    global_vars.filter_size_bytes = table_vars.filter_size

    creation_event = \
        test_utils.create_event(
            table_vars.job_id, cf_names, table_vars.time,
            events.EventType.TABLE_FILE_CREATION, table_vars.cf,
            file_number=table_vars.file_number,
            table_properties=test_utils.get_table_properties(global_vars))
    assert monitor.new_event(creation_event)
    return creation_event


def delete_test_file(monitor, table_vars):
    assert isinstance(table_vars, DeleteTestVars)

    deletion_event = \
        test_utils.create_event(table_vars.job_id, cf_names, table_vars.time,
                                events.EventType.TABLE_FILE_DELETION,
                                table_vars.cf,
                                file_number=table_vars.file_number)
    assert monitor.new_event(deletion_event)


def test_live_file_stats():
    table_vars = test_utils.TablePropertiesTestVars()

    cf1_create_1_vars = \
        CreateTestVars(cf=cf1,
                       total_keys_sizes_bytes=5,
                       total_values_sizes_bytes=15,
                       index_size=100,
                       filter_size=200)
    cf1_create_2_vars = \
        CreateTestVars(cf=cf1,
                       total_keys_sizes_bytes=10,
                       total_values_sizes_bytes=20,
                       index_size=50,
                       filter_size=500)
    cf1_create_3_vars = \
        CreateTestVars(cf=cf1,
                       total_keys_sizes_bytes=30,
                       total_values_sizes_bytes=10,
                       index_size=170,
                       filter_size=30)

    cf1_delete_1_vars = DeleteTestVars(cf1_create_1_vars)
    cf1_delete_2_vars = DeleteTestVars(cf1_create_2_vars)

    cf2_create_1_vars = \
        CreateTestVars(cf=cf2,
                       total_keys_sizes_bytes=200,
                       total_values_sizes_bytes=200,
                       index_size=100,
                       filter_size=200)
    cf2_create_2_vars = \
        CreateTestVars(cf=cf2,
                       total_keys_sizes_bytes=600,
                       total_values_sizes_bytes=400,
                       index_size=400,
                       filter_size=200)

    cf2_delete_1_vars = \
        DeleteTestVars(cf2_create_1_vars)

    monitor = db_files.DbFilesMonitor()

    create_test_file(monitor, cf1_create_1_vars, table_vars)
    # Live - cf1: data=20, index=100, filter=200

    delete_test_file(monitor, cf1_delete_1_vars)
    # Live: - cf1: data=0, index=0, filter=0

    cf1_create_2_event =\
        create_test_file(monitor, cf1_create_2_vars, table_vars)
    cf1_create_2_time = cf1_create_2_event.get_log_time()
    # Live: - cf1: data=30, index=50, filter=500

    cf2_create_1_event =\
        create_test_file(monitor, cf2_create_1_vars, table_vars)
    cf2_create_1_time = cf2_create_1_event.get_log_time()
    # Live: - cf1: data=30, index=50, filter=500,
    #         cf2: data=1000, index=100, filter=200

    cf1_create_3_event = \
        create_test_file(monitor, cf1_create_3_vars, table_vars)
    cf1_create_3_time = cf1_create_3_event.get_log_time()
    # Live: - cf1: data=70, index=220, filter=530,
    #         cf2: data=1000, index=100, filter=200

    delete_test_file(monitor, cf2_delete_1_vars)
    # Live: - cf1: data=70, index=220, filter=530,
    #         cf2: data=0, index=0, filter=0

    delete_test_file(monitor, cf1_delete_2_vars)
    # Live: - cf1: data=40, index=170, filter=30,
    #         cf2: data=0, index=0, filter=0

    cf2_create_2_event =\
        create_test_file(monitor, cf2_create_2_vars, table_vars)
    cf2_create_2_time = cf2_create_2_event.get_log_time()
    # Live: - cf1: data=40, index=170, filter=30,
    #         cf2: data=600, index=400, filter=200

    expected_cf1_data_stats = db_files.BlockLiveFileStats()
    expected_cf1_data_stats.num_created = 3
    expected_cf1_data_stats.num_live = 1
    expected_cf1_data_stats.total_created_size_bytes = 90
    expected_cf1_data_stats.curr_total_live_size_bytes = 40
    expected_cf1_data_stats.largest_block_size_bytes = 40
    expected_cf1_data_stats.largest_block_size_time = cf1_create_3_time
    expected_cf1_data_stats.max_total_live_size_bytes = 70
    expected_cf1_data_stats.max_total_live_size_time = cf1_create_3_time

    expected_cf1_index_stats = db_files.BlockLiveFileStats()
    expected_cf1_index_stats.num_created = 3
    expected_cf1_index_stats.num_live = 1
    expected_cf1_index_stats.total_created_size_bytes = 320
    expected_cf1_index_stats.curr_total_live_size_bytes = 170
    expected_cf1_index_stats.largest_block_size_bytes = 170
    expected_cf1_index_stats.largest_block_size_time = cf1_create_3_time
    expected_cf1_index_stats.max_total_live_size_bytes = 220
    expected_cf1_index_stats.max_total_live_size_time = cf1_create_3_time

    expected_cf1_filter_stats = db_files.BlockLiveFileStats()
    expected_cf1_filter_stats.num_created = 3
    expected_cf1_filter_stats.num_live = 1
    expected_cf1_filter_stats.total_created_size_bytes = 730
    expected_cf1_filter_stats.curr_total_live_size_bytes = 30
    expected_cf1_filter_stats.largest_block_size_bytes = 500
    expected_cf1_filter_stats.largest_block_size_time = cf1_create_2_time
    expected_cf1_filter_stats.max_total_live_size_bytes = 530
    expected_cf1_filter_stats.max_total_live_size_time = cf1_create_3_time

    expected_cf2_data_stats = db_files.BlockLiveFileStats()
    expected_cf2_data_stats.num_created = 2
    expected_cf2_data_stats.num_live = 1
    expected_cf2_data_stats.total_created_size_bytes = 1400
    expected_cf2_data_stats.curr_total_live_size_bytes = 1000
    expected_cf2_data_stats.largest_block_size_bytes = 1000
    expected_cf2_data_stats.largest_block_size_time = cf2_create_2_time
    expected_cf2_data_stats.max_total_live_size_bytes = 1000
    expected_cf2_data_stats.max_total_live_size_time = cf2_create_2_time

    expected_cf2_index_stats = db_files.BlockLiveFileStats()
    expected_cf2_index_stats.num_created = 2
    expected_cf2_index_stats.num_live = 1
    expected_cf2_index_stats.total_created_size_bytes = 500
    expected_cf2_index_stats.curr_total_live_size_bytes = 400
    expected_cf2_index_stats.largest_block_size_bytes = 400
    expected_cf2_index_stats.largest_block_size_time = cf2_create_2_time
    expected_cf2_index_stats.max_total_live_size_bytes = 400
    expected_cf2_index_stats.max_total_live_size_time = cf2_create_2_time

    expected_cf2_filter_stats = db_files.BlockLiveFileStats()
    expected_cf2_filter_stats.num_created = 2
    expected_cf2_filter_stats.num_live = 1
    expected_cf2_filter_stats.total_created_size_bytes = 400
    expected_cf2_filter_stats.curr_total_live_size_bytes = 200
    expected_cf2_filter_stats.largest_block_size_bytes = 200
    expected_cf2_filter_stats.largest_block_size_time = cf2_create_1_time
    expected_cf2_filter_stats.max_total_live_size_bytes = 200
    expected_cf2_filter_stats.max_total_live_size_time = cf2_create_1_time

    actual_stats = monitor.get_blocks_stats()
    assert actual_stats[cf1][db_files.BlockType.DATA] == \
           expected_cf1_data_stats
    assert actual_stats[cf1][db_files.BlockType.INDEX] == \
           expected_cf1_index_stats
    assert actual_stats[cf1][db_files.BlockType.FILTER] == \
           expected_cf1_filter_stats

    assert actual_stats[cf2][db_files.BlockType.DATA] == \
           expected_cf2_data_stats
    assert actual_stats[cf2][db_files.BlockType.INDEX] == \
           expected_cf2_index_stats
    assert actual_stats[cf2][db_files.BlockType.FILTER] == \
           expected_cf2_filter_stats

    expected_data_stats = db_files.BlockLiveFileStats()
    expected_data_stats.num_created = 5
    expected_data_stats.num_live = 2
    expected_data_stats.total_created_size_bytes = 1490
    expected_data_stats.curr_total_live_size_bytes = 1040
    expected_data_stats.largest_block_size_bytes = 1000
    expected_data_stats.largest_block_size_time = cf2_create_2_time
    # These are incorrect for more than 1 cf
    expected_data_stats.max_total_live_size_bytes = 0
    expected_data_stats.max_total_live_size_time = None

    actual_index_stats = \
        db_files.get_block_stats_for_cfs_group(
            [cf1, cf2], monitor, db_files.BlockType.DATA)
    assert actual_index_stats == expected_data_stats

    expected_index_stats = db_files.BlockLiveFileStats()
    expected_index_stats.num_created = 5
    expected_index_stats.num_live = 2
    expected_index_stats.total_created_size_bytes = 820
    expected_index_stats.curr_total_live_size_bytes = 570
    expected_index_stats.largest_block_size_bytes = 400
    expected_index_stats.largest_block_size_time = cf2_create_2_time
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
    expected_filter_stats.largest_block_size_bytes = 500
    expected_filter_stats.largest_block_size_time = cf1_create_2_time
    # These are incorrect for more than 1 cf
    # expected_filter_stats.max_total_live_size_bytes = 530
    # expected_filter_stats.max_total_live_size_time = cf1_create_3_time

    actual_filter_stats = \
        db_files.get_block_stats_for_cfs_group(
            [cf1, cf2], monitor, db_files.BlockType.FILTER)
    assert actual_filter_stats == expected_filter_stats
