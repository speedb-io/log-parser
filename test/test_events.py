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

from datetime import datetime, timedelta

import pytest

import events
import utils
from log_entry import LogEntry
from test.testing_utils import create_event_entry, create_event, \
    entry_to_event, entry_msg_to_entry


def test_try_parse_as_flush_preamble():
    cf = "cf1"
    job_id = 38
    wal_id = 55

    valid_preamble = \
        f"[{cf}] [JOB {job_id}] Flushing memtable with next log file: {wal_id}"
    partial1 = f"[{cf}] [JOB {job_id}] Flushing"
    partial2 = f"[{cf}] [JOB {job_id}] Flushing memtable with next log file:"

    test_func = events.FlushStartedEvent.try_parse_as_preamble
    assert test_func("") == (False, None, None, None)
    assert test_func(valid_preamble) == (True, cf, job_id, wal_id)
    assert test_func(partial1) == (False, None, None, None)
    assert test_func(partial2) == (False, None, None, None)


def test_try_parse_as_compaction_preamble():
    cf = "cf2"
    job_id = 157

    valid_preamble = \
        f"[{cf}] [JOB {job_id}] Compacting 1@1 + 5@2 files to L2, score 1.63"
    partial1 = \
        f"[{cf}] [JOB {job_id}] Compacting"
    partial2 = \
        f"[{cf}] [JOB {job_id}] Compacting 1@1 + 5@2 files"

    test_func = events.CompactionStartedEvent.try_parse_as_preamble
    assert test_func("") == (False, None, None)
    assert test_func(valid_preamble) == (True, cf, job_id)
    assert test_func(partial1) == (False, None, None)
    assert test_func(partial2) == (False, None, None)


def test_try_parse_event_preamble():
    cf = "cf1"
    job_id = 38
    wal_id = 55
    time_str = "2022/04/17-14:42:19.220573"

    valid_flush_preamble = \
        f"[{cf}] [JOB {job_id}] Flushing memtable with next log file: {wal_id}"
    flush_preamble_entry = entry_msg_to_entry(time_str, valid_flush_preamble)

    valid_compaction_preamble = \
        f"[{cf}] [JOB {job_id}] Compacting 1@1 + 5@2 files to L2, score 1.63"
    compaction_preamble_entry =\
        entry_msg_to_entry(time_str, valid_compaction_preamble)

    partial_compaction_preamble = f"[{cf}] [JOB {job_id}] Compacting"
    invalid_compaction_preamble_entry =\
        entry_msg_to_entry(time_str, partial_compaction_preamble)

    test_func = events.Event.try_parse_as_preamble
    info = events.Event.EventPreambleInfo

    assert test_func(flush_preamble_entry) == \
           info(cf, events.EventType.FLUSH_STARTED, job_id, wal_id=wal_id)
    assert test_func(compaction_preamble_entry) == \
           info(cf, events.EventType.COMPACTION_STARTED, job_id, wal_id=None)

    assert test_func(invalid_compaction_preamble_entry) is None


def test_compaction_started_event():
    time = "2023/01/24-08:54:40.130553"
    job_id = 1
    cf = "cf1"
    cf_names = [cf]
    reason = "Reason1"
    files_l1 = [17248]
    files_l2 = [16778, 16779, 16780, 16781, 17022]

    event = create_event(job_id, cf_names, time,
                         events.EventType.COMPACTION_STARTED, cf,
                         compaction_reason=reason,
                         files_L1=files_l1, files_L2=files_l2)

    assert isinstance(event, events.CompactionStartedEvent)
    assert event.get_compaction_reason() == reason
    assert event.get_input_files() == {
        1: files_l1,
        2: files_l2
    }


def test_table_file_creation_event():
    time = "2023/01/24-08:54:40.130553"
    file_number = 1234
    job_id = 1
    cf = "cf1"
    cf_names = [cf]
    table_properties = {}
    event = create_event(job_id, cf_names, time,
                         events.EventType.TABLE_FILE_CREATION, cf,
                         file_number=file_number,
                         table_properties=table_properties)

    assert event.get_type() == events.EventType.TABLE_FILE_CREATION.value
    assert event.get_created_file_number() == file_number
    assert event.get_cf_id() is None
    assert event.get_compressed_data_size_bytes() == 0
    assert event.get_num_data_blocks() == 0
    assert event.get_total_keys_sizes_bytes() == 0
    assert event.get_total_values_sizes_bytes() == 0
    assert event.get_data_size_bytes() == 0
    assert event.get_index_size_bytes() == 0
    assert event.get_filter_size_bytes() == 0
    assert not event.does_use_filter()
    assert event.get_num_filter_entries() == 0
    assert event.get_compression_type() is None

    table_properties2 = {
        "column_family_id": 1,
        "data_size": 100,
        "index_size": 200,
        "filter_size": 300,
        "raw_key_size": 400,
        "raw_value_size": 500,
        "num_data_blocks": 600,
        "num_entries": 700,
        "compression": "NoCompression",
        "filter_policy": "BloomFilter",
        "num_filter_entries": 800}

    event2 = create_event(job_id, cf_names, time,
                          events.EventType.TABLE_FILE_CREATION, cf,
                          file_number=file_number,
                          table_properties=table_properties2)

    assert event2.get_type() == events.EventType.TABLE_FILE_CREATION.value
    assert event2.get_created_file_number() == file_number
    assert event2.get_cf_id() == 1
    assert event2.get_compressed_data_size_bytes() == 100
    assert event2.get_num_data_blocks() == 600
    assert event2.get_total_keys_sizes_bytes() == 400
    assert event2.get_total_values_sizes_bytes() == 500
    assert event2.get_data_size_bytes() == 900
    assert event2.get_index_size_bytes() == 200
    assert event2.get_filter_size_bytes() == 300
    assert event2.does_use_filter()
    assert event2.get_filter_policy() == "BloomFilter"
    assert event2.get_num_filter_entries() == 800
    assert event2.get_compression_type() == "NoCompression"


def test_table_file_deletion_event():
    time = "2023/01/24-08:54:40.130553"
    file_number = 1234
    job_id = 1
    cf = "cf1"
    cf_names = [cf]
    event = create_event(job_id, cf_names, time,
                         events.EventType.TABLE_FILE_DELETION, cf,
                         file_number=file_number)
    assert event.get_type() == events.EventType.TABLE_FILE_DELETION.value
    assert event.get_deleted_file_number() == file_number


def test_table_file_creation_event1():
    time = "2023/01/24-08:54:40.130553"
    file_number = 1234
    job_id = 1
    cf = "cf1"
    cf_names = [cf]
    cf_id = 100
    compressed_data_size_bytes = 62396458
    num_data_blocks = 1000
    total_keys_sizes_bytes = 2000
    total_values_sizes_bytes = 3333
    index_size = 3000
    filter_size = 4000
    num_entries = 5555
    compression_type = "NoCompression"
    table_properties = {
        "data_size": compressed_data_size_bytes,
        "index_size": index_size,
        "index_partitions": 0,
        "top_level_index_size": 0,
        "index_key_is_user_key": 1,
        "index_value_is_delta_encoded": 1,
        "filter_size": filter_size,
        "raw_key_size": total_keys_sizes_bytes,
        "raw_average_key_size": 24,
        "raw_value_size": total_values_sizes_bytes,
        "raw_average_value_size": 1000,
        "num_data_blocks": num_data_blocks,
        "num_entries": num_entries,
        "num_filter_entries": 60774,
        "num_deletions": 0,
        "num_merge_operands": 0,
        "num_range_deletions": 0,
        "format_version": 0,
        "fixed_key_len": 0,
        "filter_policy": "bloomfilter",
        "column_family_name": "default",
        "column_family_id": cf_id,
        "comparator": "leveldb.BytewiseComparator",
        "merge_operator": "nullptr",
        "prefix_extractor_name": "nullptr",
        "property_collectors": "[]",
        "compression": compression_type,
        "oldest_key_time": 1672823099,
        "file_creation_time": 1672823099,
        "slow_compression_estimated_data_size": 0,
        "fast_compression_estimated_data_size": 0,
        "db_id": "c100448c-dc04-4c74-8ab2-65d72f3aa3a8",
        "db_session_id": "4GAWIG5RIF8PQWM3NOQG",
        "orig_file_number": 37155}

    event = create_event(job_id, cf_names, time,
                         events.EventType.TABLE_FILE_CREATION, cf,
                         file_number=file_number,
                         table_properties=table_properties)

    assert event.get_type() == events.EventType.TABLE_FILE_CREATION.value
    assert event.get_created_file_number() == file_number
    assert event.get_cf_id() == cf_id
    assert event.get_compressed_data_size_bytes() == compressed_data_size_bytes
    assert event.get_num_data_blocks() == num_data_blocks
    assert event.get_total_keys_sizes_bytes() == total_keys_sizes_bytes
    assert event.get_total_values_sizes_bytes() == total_values_sizes_bytes
    assert event.get_data_size_bytes() ==\
           total_keys_sizes_bytes + total_values_sizes_bytes
    assert event.get_index_size_bytes() == index_size
    assert event.get_filter_size_bytes() == filter_size
    assert event.get_compression_type() == compression_type


def verify_expected_events(events_mngr, expected_events_dict):
    # Expecting a dictionary of:
    # {<cf_name>: [<events entries for this cf>]}
    cf_names = list(expected_events_dict.keys())

    # prepare the expected events per (cf, event type)
    expected_cf_events_per_type = dict()
    for name in cf_names:
        expected_cf_events_per_type[name] = {event_type: [] for event_type
                                             in events.EventType}

    for cf_name, cf_events_entries in expected_events_dict.items():
        expected_cf_events = \
            [events.Event(event_entry) for event_entry in
             cf_events_entries]
        assert events_mngr.get_cf_events(cf_name) == expected_cf_events

        for event in expected_cf_events:
            expected_cf_events_per_type[cf_name][event.get_type()].append(
                event)

        for event_type in events.EventType:
            assert events_mngr.get_cf_events_by_type(cf_name, event_type) ==\
                   expected_cf_events_per_type[cf_name][event_type]


def test_event_type():
    assert events.EventType.type_from_str("flush_finished") == \
           events.EventType.FLUSH_FINISHED
    assert str(events.EventType.type_from_str("flush_finished")) == \
           "flush_finished"

    assert events.EventType.type_from_str("Dummy") == events.EventType.UNKNOWN
    assert str(events.EventType.type_from_str("Dummy")) == "UNKNOWN"


def test_get_matching_type_info_if_exists():
    assert events.Event.\
           get_matching_type_info_if_exists(
            events.EventType.FLUSH_STARTED) == \
           events.MatchingEventTypeInfo(
               events.EventType.FLUSH_FINISHED, events.FlowType.FLUSH, False)
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.FLUSH_FINISHED) == events.MatchingEventTypeInfo(
        events.EventType.FLUSH_STARTED, events.FlowType.FLUSH, True)
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.COMPACTION_STARTED) == \
        events.MatchingEventTypeInfo(
            events.EventType.COMPACTION_FINISHED, events.FlowType.COMPACTION,
            False)
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.COMPACTION_FINISHED) == \
        events.MatchingEventTypeInfo(events.EventType.COMPACTION_STARTED,
                                     events.FlowType.COMPACTION, True)
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.RECOVERY_STARTED) == \
        events.MatchingEventTypeInfo(events.EventType.RECOVERY_FINISHED,
                                     events.FlowType.RECOVERY, False)
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.RECOVERY_FINISHED) == \
        events.MatchingEventTypeInfo(events.EventType.RECOVERY_STARTED,
                                     events.FlowType.RECOVERY, True)

    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.TABLE_FILE_CREATION) is None
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.TABLE_FILE_DELETION) is None
    assert events.Event.\
           get_matching_type_info_if_exists(events.EventType.TRIVIAL_MOVE) \
           is None
    assert events.Event.\
           get_matching_type_info_if_exists(events.EventType.INGEST_FINISHED) \
           is None
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.BLOB_FILE_CREATION) is None
    assert events.Event.get_matching_type_info_if_exists(
        events.EventType.BLOB_FILE_DELETION) is None
    assert events.Event.\
           get_matching_type_info_if_exists(events.EventType.UNKNOWN) is None


def test_matching_event_info_get_duration_ms():
    cf_names = ["default"]
    job_id = 1
    start_event_time = "2023/01/04-08:54:59.130996"
    start_event = create_event(
        job_id, cf_names, start_event_time,
        event_type=events.EventType.FLUSH_STARTED, flush_reason="Reason1")
    start_event_info = \
        events.MatchingEventInfo(start_event, events.FlowType.FLUSH,
                                 is_start=True)

    start_datetime = datetime.strptime(f"{start_event_time}GMT",
                                       "%Y/%m/%d-%H:%M:%S.%f%Z")
    delta_ms = 1500
    time_delta_ms = timedelta(milliseconds=delta_ms)
    end_datetime = start_datetime + time_delta_ms
    end_event_time = end_datetime.strftime("%Y/%m/%d-%H:%M:%S.%f")
    end_event = create_event(
        job_id, cf_names, end_event_time,
        event_type=events.EventType.FLUSH_FINISHED)
    end_event_info = events.MatchingEventInfo(
        end_event, events.FlowType.FLUSH, is_start=False)

    assert start_event_info.get_duration_ms(end_event_info) == delta_ms
    assert end_event_info.get_duration_ms(start_event_info) == delta_ms


@pytest.mark.parametrize("cf_name", ["default", ""])
def test_event(cf_name):
    job_id = 35
    event_entry = create_event_entry(job_id, "2022/04/17-14:42:19.220573",
                                     events.EventType.FLUSH_FINISHED, cf_name)

    assert events.Event.is_an_event_entry(event_entry)
    assert not events.Event.try_parse_as_preamble(event_entry)

    event1 = events.Event(event_entry)
    assert event1.get_type() == events.EventType.FLUSH_FINISHED
    assert event1.get_job_id() == 35
    assert event1.get_cf_name() == cf_name
    assert not event1.is_db_wide_event()
    assert event1.is_cf_event()
    assert event1.get_my_matching_type_info_if_exists() == \
           events.MatchingEventTypeInfo(
               events.EventType.FLUSH_STARTED, events.FlowType.FLUSH, True)
    event1 = None

    # Unknown event (legal)
    event_entry = create_event_entry(job_id, "2022/04/17-14:42:19.220573",
                                     events.EventType.UNKNOWN, cf_name)
    event2 = events.Event(event_entry)
    assert event2.get_type() == events.EventType.UNKNOWN
    assert event2.get_job_id() == 35
    assert event2.get_cf_name() == cf_name
    assert not event2.is_db_wide_event()
    assert event2.is_cf_event()
    assert event2.get_my_matching_type_info_if_exists() is None


@pytest.mark.parametrize("cf_name", ["default", ""])
def test_event_preamble(cf_name):
    preamble_line = f"""2022/04/17-14:42:11.398681 7f4a8b5bb700 
    [/flush_job.cc:333] [{cf_name}] [JOB 8] 
    Flushing memtable with next log file: 5
    """ # noqa

    preamble_line = " ".join(preamble_line.splitlines())

    cf_names = [cf_name, "dummy_cf"]
    job_id = 8

    preamble_entry = LogEntry(0, preamble_line, True)
    event_entry = create_event_entry(job_id, "2022/11/24-15:58:17.683316",
                                     events.EventType.FLUSH_STARTED,
                                     utils.NO_CF, flush_reason="REASON1")

    assert not events.Event.try_parse_as_preamble(event_entry)

    preamble_info = \
        events.Event.try_parse_as_preamble(preamble_entry)
    assert preamble_info
    assert preamble_info.job_id == 8
    assert preamble_info.type == events.EventType.FLUSH_STARTED
    assert preamble_info.cf_name == cf_name

    assert events.Event.is_an_event_entry(event_entry)
    assert not events.Event.try_parse_as_preamble(event_entry)

    event = events.Event.create_event(event_entry)
    assert event.get_type() == events.EventType.FLUSH_STARTED
    assert event.get_job_id() == 8
    assert event.get_cf_name() == utils.NO_CF
    assert event.is_db_wide_event()
    assert not event.is_cf_event()
    assert event.get_wal_id_if_available() is None
    assert event.get_flush_reason() == "REASON1"

    assert event.try_adding_preamble_event(preamble_info)
    assert event.get_cf_name() == cf_name
    assert not event.is_db_wide_event()
    assert event.is_cf_event()
    assert event.get_wal_id_if_available() == 5
    assert event.get_my_matching_type_info_if_exists() == \
           events.MatchingEventTypeInfo(
               events.EventType.FLUSH_FINISHED, events.FlowType.FLUSH, False)
    assert event.get_matching_event_info_if_exists() is None

    compaction_finished_event = \
        create_event(job_id, cf_names, "2022/11/24-15:59:17.683316",
                     events.EventType.COMPACTION_FINISHED, cf_name)
    event.try_adding_matching_event(compaction_finished_event)
    assert event.get_matching_event_info_if_exists() is None

    flush_end_event = create_event(job_id, cf_names,
                                   "2022/11/24-15:59:17.683316",
                                   events.EventType.FLUSH_FINISHED,
                                   cf_name)
    event.try_adding_matching_event(flush_end_event)
    assert event.get_matching_event_info_if_exists() ==\
           events.MatchingEventInfo(flush_end_event, events.FlowType.FLUSH,
                                    is_start=False)


def test_illegal_events():
    cf1 = "CF1"

    # Illegal event json
    event_entry = create_event_entry(35, "2022/04/17-14:42:19.220573",
                                     events.EventType.FLUSH_FINISHED, cf1,
                                     make_illegal_json=True)
    assert events.Event.try_parse_as_preamble(event_entry) is None
    with pytest.raises(utils.ParsingError):
        events.Event.create_event(event_entry)

    # Missing Job id (illegal)
    event_entry = create_event_entry(None, "2022/04/17-14:42:19.220573",
                                     events.EventType.FLUSH_FINISHED,
                                     cf_name=cf1)
    assert events.Event.try_parse_as_preamble(event_entry) is None
    assert events.Event.is_an_event_entry(event_entry)
    with pytest.raises(utils.ParsingError):
        events.Event.create_event(event_entry)

    # Missing events.Event Type (definitely illegal)
    event_entry = create_event_entry(200, "2022/04/17-14:42:19.220573",
                                     event_type=None, cf_name=cf1)
    assert events.Event.try_parse_as_preamble(event_entry) is None
    assert events.Event.is_an_event_entry(event_entry)
    with pytest.raises(utils.ParsingError):
        events.Event.create_event(event_entry)


def test_handling_same_job_id_multiple_cfs():
    cf1 = "cf1"
    cf2 = "cf2"
    event1_time = "2023/01/04-08:54:59.130996"
    event2_time = "2023/01/04-08:55:59.130996"
    event3_time = "2023/01/04-08:56:59.130996"
    job_id = 1
    job_id_to_cf_name_map = {job_id: cf1}

    events_mngr = events.EventsMngr(job_id_to_cf_name_map)

    event_entry_cf1 = create_event_entry(job_id, event1_time,
                                         events.EventType.FLUSH_STARTED, cf1,
                                         flush_reason="FLUSH_REASON1")
    event_cf1 = entry_to_event(event_entry_cf1)
    assert events_mngr.try_adding_entry(event_entry_cf1) ==\
           (True, event_cf1, cf1)

    event_entry_cf2 = create_event_entry(job_id, event2_time,
                                         events.EventType.FLUSH_FINISHED, cf2)
    assert events_mngr.try_adding_entry(event_entry_cf2) == (True, None, None)

    # Now adding without a cf => should match the event to the 1st one
    event_entry_cf3 = create_event_entry(job_id, event3_time,
                                         events.EventType.FLUSH_FINISHED,
                                         utils.NO_CF)
    event_no_cf = entry_to_event(event_entry_cf3)
    event_no_cf.set_cf_name(cf1)
    assert events_mngr.try_adding_entry(event_entry_cf3) == \
           (True, event_no_cf, cf1)


def test_adding_events_to_events_mngr():
    cf1 = "cf1"
    job_id1 = 1
    cf2 = "cf2"
    job_id2 = 2
    job_id_to_cf_name_map = {job_id1: cf1, job_id2: cf2}
    events_mngr = events.EventsMngr(job_id_to_cf_name_map)

    assert not events_mngr.get_cf_events(cf1)
    assert not events_mngr.\
        get_cf_events_by_type(cf2, events.EventType.FLUSH_FINISHED)

    expected_events_entries = {cf1: [], cf2: []}

    event1_entry = create_event_entry(job_id1, "2022/04/17-14:42:19.220573",
                                      events.EventType.FLUSH_FINISHED, cf1)
    event1 = entry_to_event(event1_entry)
    assert events_mngr.try_adding_entry(event1_entry) == (True, event1, cf1)
    expected_events_entries[cf1] = [event1_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    event2_entry = create_event_entry(job_id2, "2022/04/18-14:42:19.220573",
                                      events.EventType.FLUSH_STARTED, cf2,
                                      flush_reason="FLUSH_REASON1")
    event2 = entry_to_event(event2_entry)
    assert events_mngr.try_adding_entry(event2_entry) == (True, event2, cf2)
    expected_events_entries[cf2] = [event2_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    # Create another cf1 event, but set its time to EARLIER than event1
    event3_entry = create_event_entry(job_id1, "2022/03/17-14:42:19.220573",
                                      events.EventType.FLUSH_FINISHED, cf1)
    event3 = entry_to_event(event3_entry)
    assert events_mngr.try_adding_entry(event3_entry) == (True, event3, cf1)
    # Expecting event3 to be before event1
    expected_events_entries[cf1] = [event3_entry, event1_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    # Create some more cf21 event, later in time
    event4_entry = \
        create_event_entry(job_id2, "2022/05/17-14:42:19.220573",
                           events.EventType.COMPACTION_STARTED, cf2,
                           compaction_reason="Reason1")
    event4 = entry_to_event(event4_entry)
    event5_entry = \
        create_event_entry(job_id2, "2022/05/17-15:42:19.220573",
                           events.EventType.COMPACTION_STARTED, cf2,
                           compaction_reason="Reason2")
    event5 = entry_to_event(event5_entry)
    event6_entry =\
        create_event_entry(job_id2, "2022/05/17-16:42:19.220573",
                           events.EventType.COMPACTION_FINISHED, cf2)
    event6 = entry_to_event(event6_entry)
    assert events_mngr.try_adding_entry(event4_entry) == (True, event4, cf2)
    assert events_mngr.try_adding_entry(event5_entry) == (True, event5, cf2)
    assert events_mngr.try_adding_entry(event6_entry) == (True, event6, cf2)
    expected_events_entries[cf2] = [event2_entry, event4_entry,
                                    event5_entry, event6_entry]
    verify_expected_events(events_mngr, expected_events_entries)


def test_get_flow_events():
    cf1 = "cf1"
    cf2 = "cf2"
    cfs_names = [cf1, cf2]
    job_id = 1
    job_id_to_cf_name_map = {job_id: cf1}
    events_mngr = events.EventsMngr(job_id_to_cf_name_map)

    assert events_mngr.get_cf_flow_events(events.FlowType.FLUSH, cf1) == []
    assert events_mngr.get_cf_flow_events(events.FlowType.FLUSH, cf2) == []
    assert events_mngr.get_all_flow_events(events.FlowType.FLUSH, cfs_names)\
           == []

    flush_started_cf1_1 = create_event_entry(job_id,
                                             "2022/04/18-14:42:19.220573",
                                             events.EventType.FLUSH_STARTED,
                                             cf1,
                                             flush_reason="FLUSH_REASON1")
    flush_started_event = entry_to_event(flush_started_cf1_1)
    assert events_mngr.try_adding_entry(flush_started_cf1_1) == \
           (True, flush_started_event, cf1)
    cf1_flush_started_events =\
        events_mngr.get_cf_events_by_type(cf1, events.EventType.FLUSH_STARTED)

    expected_flush_events = [(cf1_flush_started_events[0], None)]
    assert events_mngr.get_cf_flow_events(events.FlowType.FLUSH, cf1) == \
           expected_flush_events
    assert events_mngr.get_all_flow_events(events.FlowType.FLUSH, cfs_names)\
           == expected_flush_events

    flush_finished_cf1_1 = \
        create_event_entry(job_id,
                           "2022/04/18-14:43:19.220573",
                           events.EventType.FLUSH_FINISHED,
                           cf1)
    flush_finished_event = entry_to_event(flush_finished_cf1_1)
    assert events_mngr.try_adding_entry(flush_finished_cf1_1) == \
           (True, flush_finished_event, cf1)
    cf1_flush_started_events =\
        events_mngr.get_cf_events_by_type(cf1, events.EventType.FLUSH_STARTED)
    cf1_flush_finished_events =\
        events_mngr.get_cf_events_by_type(cf1, events.EventType.FLUSH_FINISHED)

    expected_flush_events = [(cf1_flush_started_events[0],
                              cf1_flush_finished_events[0])]
    assert events_mngr.get_all_flow_events(events.FlowType.FLUSH, cfs_names)\
           == expected_flush_events


def test_try_adding_invalid_event_to_events_mngr():
    cf1 = "cf1"
    job_id = 35
    job_id_to_cf_name_map = {job_id: cf1}
    events_mngr = events.EventsMngr(job_id_to_cf_name_map)

    # Illegal event json
    invalid_event_entry =\
        create_event_entry(job_id,
                           "2022/04/17-14:42:19.220573",
                           events.EventType.FLUSH_FINISHED,
                           cf1,
                           make_illegal_json=True)

    assert events_mngr.try_adding_entry(invalid_event_entry) == \
           (True, None, None)
    assert events_mngr.debug_get_all_events() == {}

    event1_entry = create_event_entry(job_id, "2022/04/17-14:42:19.220573",
                                      events.EventType.FLUSH_FINISHED, cf1)
    event1 = entry_to_event(event1_entry)
    assert events_mngr.try_adding_entry(event1_entry) == (True, event1, cf1)
    assert len(events_mngr.debug_get_all_events()) == 1

    assert events_mngr.try_adding_entry(invalid_event_entry) == \
           (True, None, None)
    assert len(events_mngr.debug_get_all_events()) == 1
