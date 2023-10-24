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

import json
import logging
import re
import typing
from dataclasses import dataclass
from enum import Enum

import regexes
import utils
from log_entry import LogEntry


class EventType(str, Enum):
    FLUSH_STARTED = "flush_started"
    FLUSH_FINISHED = "flush_finished"
    COMPACTION_STARTED = "compaction_started"
    COMPACTION_FINISHED = "compaction_finished"
    TABLE_FILE_CREATION = 'table_file_creation'
    TABLE_FILE_DELETION = "table_file_deletion"
    TRIVIAL_MOVE = "trivial_move"
    RECOVERY_STARTED = "recovery_started"
    RECOVERY_FINISHED = "recovery_finished"
    INGEST_FINISHED = "ingest_finished"
    BLOB_FILE_CREATION = "blob_file_creation"
    BLOB_FILE_DELETION = "blob_file_deletion"
    UNKNOWN = "UNKNOWN"

    def __str__(self):
        return str(self.value)

    @staticmethod
    def type_from_str(event_type_str):
        try:
            return EventType(event_type_str)
        except ValueError:
            return EventType.UNKNOWN


class EventField(str, Enum):
    EVENT_TYPE = "event"
    JOB_ID = "job"
    TIME_MICROS = "time_micros"
    CF_NAME = "cf_name"
    FLUSH_REASON = "flush_reason"
    COMPACTION_REASON = "compaction_reason"
    FILE_NUMBER = "file_number"
    FILE_SIZE = "file_size"
    TABLE_PROPERTIES = "table_properties"
    WAL_ID = "wal_id",
    NUM_ENTRIES = "num_entries"
    NUM_DELETES = "num_deletes"
    NUM_MEMTABLES = "num_memtables"
    TOTAL_DATA_SIZE = "total_data_size",
    INPUT_DATA_SIZE = "input_data_size"
    COMPACTION_TIME_MICROS = "compaction_time_micros"
    TOTAL_OUTPUT_SIZE = "total_output_size"
    # Table Creation
    OLDEST_BLOB_FILE_NUM = "oldest_blob_file_number"
    # Compaction Finished
    OUTPUT_LEVEL = "output_level"
    NUM_OUTPUT_FILES = "num_output_files"
    NUM_INPUT_RECORDS = "num_input_records"
    RECORDS_IN = "records_in"
    RECORDS_DROPPED = "records_dropped"


class TablePropertiesField(str, Enum):
    CF_ID = "column_family_id"
    DATA_SIZE = "data_size"
    INDEX_SIZE = "index_size"
    FILTER_SIZE = "filter_size"
    FILTER_POLICY = "filter_policy"
    NUM_FILTER_ENTRIES = "num_filter_entries"
    NUM_DATA_BLOCKS = "num_data_blocks"
    TOTAL_KEY_SIZE = "raw_key_size"
    TOTAL_VALUE_SIZE = "raw_value_size"
    COMPRESSION_TYPE = "compression"


class FlowType(str, Enum):
    FLUSH = "Flush"
    COMPACTION = "Compaction"
    RECOVERY = "Recovery"

    def __str__(self):
        return str(self.value)


# Events that have a start and finish events that should be matched
# based on their job-id
# Every tuple consists of:
# - The event in the log marking the start
# - The event in the log marking the end
# - The type of the associated operation / flow (e.g., Flush)
# The duration of the associated operation is the time difference
# between the 2
MATCHING_EVENTS = [
    (EventType.FLUSH_STARTED, EventType.FLUSH_FINISHED, FlowType.FLUSH),
    (EventType.COMPACTION_STARTED, EventType.COMPACTION_FINISHED,
     FlowType.COMPACTION),
    (EventType.RECOVERY_STARTED, EventType.RECOVERY_FINISHED,
     FlowType.RECOVERY)
]


Event = typing.NewType("Event", None)


@dataclass
class MatchingEventTypeInfo:
    event_type: EventType
    associated_flow_type: FlowType
    is_start: bool


@dataclass
class MatchingEventInfo:
    event: Event
    associated_flow_type: FlowType
    is_start: bool

    def get_duration_ms(self, matching_event_info):
        if self.is_start:
            start_time_epoch_micro = \
                self.event.get_time_since_epoch_microseconds()
            end_time_epoch_micro = \
                matching_event_info.event.get_time_since_epoch_microseconds()
        else:
            start_time_epoch_micro = \
                matching_event_info.event.get_time_since_epoch_microseconds()
            end_time_epoch_micro = \
                self.event.get_time_since_epoch_microseconds()

        assert end_time_epoch_micro >= start_time_epoch_micro
        return int((end_time_epoch_micro - start_time_epoch_micro) / 1000)


def get_flow_start_event_type(flow_type):
    for match in MATCHING_EVENTS:
        if flow_type == match[2]:
            return match[0]
    return None


class Event:
    @dataclass
    class EventPreambleInfo:
        cf_name: str
        type: str
        job_id: str
        wal_id: str = None

        def are_equal_ignoring_wal_id(self, other):
            return self.cf_name == other.cf_name and \
                   self.type == other.type and \
                   self.job_id == other.job_id

    @staticmethod
    def is_an_event_entry(log_entry):
        assert isinstance(log_entry, LogEntry)
        return re.findall(regexes.EVENT, log_entry.get_msg()) != []

    @staticmethod
    def try_parse_as_preamble(log_entry):
        event_msg = log_entry.get_msg()

        is_preamble, cf_name, job_id, wal_id = \
            FlushStartedEvent.try_parse_as_preamble(event_msg)
        if is_preamble:
            event_type = EventType.FLUSH_STARTED
        else:
            wal_id = None

        if not is_preamble:
            is_preamble, cf_name, job_id = \
                CompactionStartedEvent.try_parse_as_preamble(event_msg)
            if is_preamble:
                event_type = EventType.COMPACTION_STARTED

        if not is_preamble:
            return None

        return Event.EventPreambleInfo(cf_name, event_type, job_id, wal_id)

    @staticmethod
    def create_event(log_entry):
        assert Event.is_an_event_entry(log_entry)

        entry_msg = log_entry.get_msg()
        event_json_str = entry_msg[entry_msg.find("{"):]

        try:
            event_details_dict = json.loads(event_json_str)
        except json.JSONDecodeError:
            raise utils.ParsingError(
                f"Error decoding event's json fields.n{log_entry}")

        event_type = Event.get_event_data_field(
            event_details_dict, EventField.EVENT_TYPE)

        if event_type == EventType.FLUSH_STARTED:
            event = FlushStartedEvent(log_entry)
        elif event_type == EventType.FLUSH_FINISHED:
            event = FlushFinishedEvent(log_entry)
        elif event_type == EventType.COMPACTION_STARTED:
            event = CompactionStartedEvent(log_entry)
        elif event_type == EventType.COMPACTION_FINISHED:
            event = CompactionFinishedEvent(log_entry)
        elif event_type == EventType.TABLE_FILE_CREATION:
            event = TableFileCreationEvent(log_entry)
        elif event_type == EventType.TABLE_FILE_DELETION:
            event = TableFileDeletionEvent(log_entry)
        else:
            raise utils.ParsingError(
                f"Unsupported event type ({event_type}).\n{log_entry}")

        if not event.is_valid():
            raise utils.ParsingError(
                f"Invalid event (Probably missing an Event Field) ("
                f"Mandatory: {event.get_all_mandatory_fields()}.\
                n{log_entry}")

        return event

    def __init__(self, log_entry):
        assert Event.is_an_event_entry(log_entry)

        entry_msg = log_entry.get_msg()
        event_json_str = entry_msg[entry_msg.find("{"):]

        self.log_time_str = log_entry.get_time()
        self.matching_event_info = None
        self.event_details_dict = None
        self.cf_name = None

        try:
            self.event_details_dict = json.loads(event_json_str)
        except json.JSONDecodeError:
            raise utils.ParsingError(
                f"Error decoding event's json fields.n{log_entry}")

        self.cf_name = self.get_event_data_field1(
            EventField.CF_NAME, default=utils.NO_CF, field_expected=False)

    def __str__(self):
        if not self.does_have_details():
            return "Event: No Details"

        # Accessing all fields via their methods and not logging errors
        # to avoid endless recursion
        return f"Event: type: {self.get_type(default=EventType.UNKNOWN)}," \
               f"job-id: {self.get_job_id(default=utils.INVALID_JOB_ID)}, " \
               f"cf: {self.get_cf_name(utils.INVALID_CF)}"

    # By default, sort events based on their time
    def __lt__(self, other):
        return self.get_log_time() < other.get_log_time()

    def __eq__(self, other):
        print("Here")
        return self.get_log_time() == other.get_log_time() and \
            self.get_type() == other.get_type() and \
            self.get_cf_name() == other.get_cf_name()

    def does_have_details(self):
        return self.event_details_dict is not None

    def does_have_field(self, field):
        return self.get_event_data_field1(
            field, default=None, field_expected=False) is not None

    @staticmethod
    def get_common_mandatory_fields():
        return [EventField.EVENT_TYPE,
                EventField.JOB_ID]

    def get_all_mandatory_fields(self):
        all_mandatory_fields = self.get_mandatory_fields()
        all_mandatory_fields.extend(Event.get_common_mandatory_fields())
        return all_mandatory_fields

    def is_valid(self):
        mandatory_fields = self.get_all_mandatory_fields()
        for field in mandatory_fields:
            if not self.does_have_field(field):
                return False

        return True

    @staticmethod
    def get_dict_field(field, fields_dict, default=None, field_expected=True):
        field_str = field.value
        if field_str not in fields_dict:
            if default is None and field_expected:
                raise utils.ParsingError(
                    f"Can't find field ({field.value}) in dict.\
                    n{fields_dict}")
            return default

        return fields_dict[field_str]

    @staticmethod
    def get_event_data_field(
            event_details_dict, field, default=None, field_expected=True):
        assert isinstance(field, EventField)

        try:
            return Event.get_dict_field(
                field, event_details_dict, default, field_expected)
        except utils.ParsingError:
            raise utils.ParsingError(
                f"Can't find field ({field.value}) in event details."
                f"\n{event_details_dict}")

    def get_event_data_field1(self, field, default=None, field_expected=True):
        return Event.get_event_data_field(self.event_details_dict, field,
                                          default, field_expected)

    def get_log_time(self):
        return self.log_time_str

    def get_type(self, default=None):
        return self.get_event_data_field1(EventField.EVENT_TYPE, default)

    def get_job_id(self, default=None):
        job_id = self.get_event_data_field1(EventField.JOB_ID, default)
        return job_id

    def get_time_since_epoch_microseconds(self, default=None):
        return self.get_event_data_field1(EventField.TIME_MICROS, default)

    def get_event_data_dict(self):
        return self.event_details_dict

    def is_db_wide_event(self):
        return self.get_cf_name() == utils.NO_CF

    def is_cf_event(self):
        return not self.is_db_wide_event()

    def get_cf_name(self, default=utils.INVALID_CF):
        return self.cf_name

    def set_cf_name(self, cf_name):
        curr_cf_name = self.get_cf_name()
        if curr_cf_name == cf_name:
            return

        if curr_cf_name != utils.NO_CF:
            raise utils.ParsingError(
                f"Trying to set cf name {cf_name} to an event that already "
                f"has a cf name ({curr_cf_name}."
                f"\n{self}")

        self.cf_name = cf_name

    def set_wal_id(self, wal_id):
        curr_wal_id = self.get_wal_id_if_available()

        if curr_wal_id and curr_wal_id != wal_id:
            raise utils.ParsingError(
                f"Trying to set wal id {wal_id} to an event that already "
                f"has a wal id ({curr_wal_id}."
                f"\n{self}")

        self.event_details_dict[EventField.WAL_ID.value] = wal_id

    def get_wal_id_if_available(self, default=None):
        return self.get_event_data_field1(EventField.WAL_ID, default,
                                          field_expected=False)

    def get_matching_event_info_if_exists(self):
        return self.matching_event_info

    def try_adding_preamble_event(self, preamble_info):
        if self.get_type() != preamble_info.type:
            return False

        # Add the cf_name as if it was part of the event
        self.set_cf_name(preamble_info.cf_name)

        if preamble_info.wal_id is not None:
            self.set_wal_id(preamble_info.wal_id)

        return True

    def set_matching_event_info(self, matching_event_info):
        assert isinstance(matching_event_info, MatchingEventInfo)

        matching_event = matching_event_info.event
        assert self.get_job_id() == matching_event.get_job_id()
        assert self.get_cf_name(default=utils.NO_CF) == \
               matching_event.get_cf_name(default=utils.NO_CF)

        if self.matching_event_info:
            logging.error(f"Already have a matching event."
                          f"\nMe:{self}"
                          f"\nCandidate:{matching_event_info.event}")

        self.matching_event_info = matching_event_info

    def get_my_matching_type_info_if_exists(self):
        return Event.get_matching_type_info_if_exists(self.get_type())

    @staticmethod
    def get_matching_type_info_if_exists(event_type):
        for match in MATCHING_EVENTS:
            if event_type == match[0]:
                return MatchingEventTypeInfo(event_type=match[1],
                                             associated_flow_type=match[2],
                                             is_start=False)
            elif event_type == match[1]:
                return MatchingEventTypeInfo(event_type=match[0],
                                             associated_flow_type=match[2],
                                             is_start=True)

        return None

    def try_adding_matching_event(self, candidate_event):
        assert isinstance(candidate_event, Event)

        my_matching_type_info = self.get_my_matching_type_info_if_exists()
        if my_matching_type_info is None:
            return False

        if my_matching_type_info.event_type != candidate_event.get_type():
            return False

        if self.get_job_id() != candidate_event.get_job_id():
            return False

        if self.get_cf_name() != candidate_event.get_cf_name():
            return False

        # The candidate is matching. Record it
        associated_flow_type = my_matching_type_info.associated_flow_type
        my_matching_event_info = \
            MatchingEventInfo(event=candidate_event,
                              associated_flow_type=associated_flow_type,
                              is_start=my_matching_type_info.is_start)
        self.set_matching_event_info(my_matching_event_info)

        candidate_matching_event_info =\
            MatchingEventInfo(event=self,
                              associated_flow_type=associated_flow_type,
                              is_start=not my_matching_event_info.is_start)
        candidate_event.set_matching_event_info(candidate_matching_event_info)

        return True


class FlushStartedEvent(Event):
    @staticmethod
    def try_parse_as_preamble(event_msg):
        # [column_family_name_000018] [JOB 38] Flushing memtable with next log file: 5 # noqa
        # Returns is_preamble, cf_name, job_id, wal_id
        match = re.search(regexes.FLUSH_EVENT_PREAMBLE, event_msg)
        if not match:
            return False, None, None, None

        assert len(match.groups()) == 3

        return True, \
            match.group('cf'), int(match.group('job_id')), \
            int(match.group('wal_id'))

    # 2023/01/04-08:55:00.625647 27424 EVENT_LOG_v1
    # {"time_micros": 1672822500625643, "job": 8,
    # "event": "flush_started",
    # "num_memtables": 1, "num_entries": 59913, "num_deletes": 0,
    # "total_data_size": 61530651, "memory_usage": 66349552,
    # "flush_reason": "Write Buffer Full"}
    #
    def __init__(self, log_entry):
        super().__init__(log_entry)

    @staticmethod
    def get_mandatory_fields():
        return [EventField.TIME_MICROS,
                EventField.FLUSH_REASON]

    def get_flush_reason(self, default=None):
        return self.get_event_data_field1(EventField.FLUSH_REASON, default)

    def get_num_entries(self, default=0):
        return self.get_event_data_field1(EventField.NUM_ENTRIES, default)

    def get_num_deletes(self, default=0):
        return self.get_event_data_field1(EventField.NUM_DELETES, default)

    def get_num_memtables(self, default=0):
        return self.get_event_data_field1(EventField.NUM_MEMTABLES, default)

    def get_total_data_size_bytes(self, default=0):
        return self.get_event_data_field1(EventField.TOTAL_DATA_SIZE, default)


class FlushFinishedEvent(Event):
    # 2023/01/04-08:55:00.743632 27424
    # (Original Log Time 2023/01/04-08:55:00.743481)
    # EVENT_LOG_v1 {"time_micros": 1672822500743473, "job": 8,
    # "event": "flush_finished", "output_compression": "NoCompression",
    # "lsm_state": [8, 3, 45, 427, 822, 0, 0], "immutable_memtables": 0}
    #
    def __init__(self, log_entry):
        super().__init__(log_entry)

    @staticmethod
    def get_mandatory_fields():
        return [EventField.TIME_MICROS]


class CompactionStartedEvent(Event):
    @staticmethod
    def try_parse_as_preamble(event_msg):
        # [default] [JOB 13] Compacting 1@1 + 5@2 files to L2, score 1.63
        # Returns is_preamble, cf_name, job_id
        match = re.search(regexes.COMPACTION_EVENT_PREAMBLE, event_msg)
        if not match:
            return False, None, None

        assert len(match.groups()) == 2

        return True, match.group('cf'), int(match.group('job_id'))

    # 2023/01/04-08:55:00.743718 27420 EVENT_LOG_v1
    # {"time_micros": 1672822500743711, "job": 9,
    # "event": "compaction_started", "compaction_reason": "LevelL0FilesNum",
    # "files_L0": [17250, 17247, 17243, 17239], "score": 1,
    # "input_data_size": 251316602}
    #
    def __init__(self, log_entry):
        super().__init__(log_entry)

    @staticmethod
    def get_mandatory_fields():
        return [EventField.TIME_MICROS,
                EventField.COMPACTION_REASON]

    def get_compaction_reason(self, default=None):
        return self.get_event_data_field1(EventField.COMPACTION_REASON,
                                          default)

    def get_input_data_size_bytes(self, default=0):
        return self.get_event_data_field1(EventField.INPUT_DATA_SIZE, default)

    def get_input_files(self):
        # returns {<level>: list(files)}
        input_files = dict()

        for level_str, files_list in self.get_event_data_dict().items():
            if level_str.startswith('files_L'):
                level = int(level_str.lstrip('files_L'))
                input_files[level] = files_list

        return input_files


class CompactionFinishedEvent(Event):
    # 2023/01/04-08:55:00.746783 27413
    # (Original Log Time 2023/01/04-08:55:00.746653) EVENT_LOG_v1
    # {"time_micros": 1672822500746645, "job": 4,
    # "event": "compaction_finished",
    # "compaction_time_micros": 971568, "compaction_time_cpu_micros": 935180,
    # "output_level": 1, "num_output_files": 7, "total_output_size": 437263613,
    # "num_input_records": 424286, "num_output_records": 423497,
    # "num_subcompactions": 1, "output_compression": "NoCompression",
    # "num_single_delete_mismatches": 0, "num_single_delete_fallthrough": 0,
    # "lsm_state": [4, 7, 45, 427, 822, 0, 0]}
    #
    def __init__(self, log_entry):
        super().__init__(log_entry)

    @staticmethod
    def get_mandatory_fields():
        return [EventField.TIME_MICROS]

    def get_num_input_records(self, default=0):
        return self.get_event_data_field1(EventField.NUM_INPUT_RECORDS,
                                          default)

    def get_compaction_duration_micros(self, default=0):
        return self.get_event_data_field1(
            EventField.COMPACTION_TIME_MICROS, default)

    def get_compaction_duration_seconds(self, default=0):
        return int(self.get_compaction_duration_micros() / 1000)

    def get_total_output_size_bytes(self, default=0):
        return self.get_event_data_field1(EventField.TOTAL_OUTPUT_SIZE,
                                          default)

    def get_output_level(self, default=utils.INVALID_LEVEL):
        return self.get_event_data_field1(EventField.OUTPUT_LEVEL, default)

    def get_num_output_files(self, default=0):
        return self.get_event_data_field1(EventField.NUM_OUTPUT_FILES, default)


class TableFileCreationEvent(Event):
    # Sample Event:
    # 2023/01/04-09:04:59.399021 27424 EVENT_LOG_v1
    # {"time_micros": 1672823099398998, "cf_name": "default", "job": 8564,
    # "event": "table_file_creation", "file_number": 37155, "file_size":
    # 62762756, "file_checksum": "", "file_checksum_func_name": "Unknown",
    #
    # "table_properties":
    # {"data_size": 62396458, "index_size": 289284,
    # "index_partitions": 0, "top_level_index_size": 0,
    # "index_key_is_user_key": 1, "index_value_is_delta_encoded": 1,
    # "filter_size": 75973, "raw_key_size": 1458576,"raw_average_key_size": 24,
    # "raw_value_size": 60774000, "raw_average_value_size": 1000,
    # "num_data_blocks": 15194, "num_entries": 60774, "num_filter_entries":
    # 60774, "num_deletions": 0, "num_merge_operands": 0,
    # "num_range_deletions": 0, "format_version": 0, "fixed_key_len": 0,
    # "filter_policy": "bloomfilter", "column_family_name": "default",
    # "column_family_id": 0, "comparator": "leveldb.BytewiseComparator",
    # "merge_operator": "nullptr", "prefix_extractor_name": "nullptr",
    # "property_collectors": "[]", "compression": "NoCompression",
    # "compression_options": "window_bits=-14; level=32767; strategy=0;
    # max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0;
    # max_dict_buffer_bytes=0; ", "creation_time": 1672823099,
    # "oldest_key_time": 1672823099, "file_creation_time": 1672823099,
    # "slow_compression_estimated_data_size": 0,
    # "fast_compression_estimated_data_size": 0,
    # "db_id": "c100448c-dc04-4c74-8ab2-65d72f3aa3a8",
    # "db_session_id": "4GAWIG5RIF8PQWM3NOQG", "orig_file_number": 37155}}
    #

    NO_FILTER = ""

    def __init__(self, log_entry):
        super().__init__(log_entry)

    @staticmethod
    def get_mandatory_fields():
        return [EventField.TIME_MICROS,
                EventField.CF_NAME,
                EventField.FILE_NUMBER,
                EventField.TABLE_PROPERTIES]

    def get_cf_id(self, default=utils.INVALID_CF_ID):
        cf_id =\
            self.get_table_properties_field(
                TablePropertiesField.CF_ID, default)
        if cf_id == utils.INVALID_CF_ID:
            return None

        return cf_id

    def get_created_file_number(self, default=None):
        return self.get_event_data_field1(EventField.FILE_NUMBER, default)

    def get_compressed_file_size_bytes(self, default=0):
        return self.get_event_data_field1(EventField.FILE_SIZE, default)

    def get_compressed_data_size_bytes(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.DATA_SIZE, default)

    def get_num_data_blocks(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.NUM_DATA_BLOCKS, default)

    def get_total_keys_sizes_bytes(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.TOTAL_KEY_SIZE, default)

    def get_total_values_sizes_bytes(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.TOTAL_VALUE_SIZE, default)

    def get_data_size_bytes(self, default=0):
        return self.get_total_keys_sizes_bytes() + \
            self.get_total_values_sizes_bytes()

    def get_index_size_bytes(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.INDEX_SIZE, default)

    def get_filter_size_bytes(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.FILTER_SIZE, default)

    def does_use_filter(self):
        filter_policy = \
            self.get_table_properties_field(
                TablePropertiesField.FILTER_POLICY,
                default=None, field_expected=False)
        return filter_policy is not None and filter_policy != \
            TableFileCreationEvent.NO_FILTER

    def get_filter_policy(self):
        if not self.does_use_filter():
            return None
        return \
            self.get_table_properties_field(TablePropertiesField.FILTER_POLICY)

    def get_num_filter_entries(self, default=0):
        return self.get_table_properties_field(
            TablePropertiesField.NUM_FILTER_ENTRIES, default)

    def get_compression_type(self, default=None):
        compression_type = \
            self.get_table_properties_field(
                TablePropertiesField.COMPRESSION_TYPE, default="")
        if compression_type == "":
            return None
        return compression_type

    def is_compressed(self):
        return self.get_compression_type() == utils.NO_COMPRESSION

    def get_table_properties(self):
        return self.get_event_data_field1(EventField.TABLE_PROPERTIES)

    def get_table_properties_field(self, table_field, default=None,
                                   field_expected=True):
        assert isinstance(table_field, TablePropertiesField)

        table_properties_dict = \
            self.get_event_data_field1(EventField.TABLE_PROPERTIES)

        try:
            return self.get_dict_field(
                table_field, table_properties_dict, default, field_expected)
        except utils.ParsingError:
            raise utils.ParsingError(
                f"{table_field} not in table properties.\n{self}")


class TableFileDeletionEvent(Event):
    # Sample Event:
    # 2023/01/04-09:05:00.808463 27416 EVENT_LOG_v1
    # {"time_micros": 1672823100808460, "job": 8423,
    # "event": "table_file_deletion", "file_number": 37162}
    #
    def __init__(self, log_entry):
        super().__init__(log_entry)

    @staticmethod
    def get_mandatory_fields():
        return [EventField.TIME_MICROS,
                EventField.FILE_NUMBER]

    def get_deleted_file_number(self, default=None):
        return self.get_event_data_field1(EventField.FILE_NUMBER, default)


class EventsMngr:
    """
    The events manager contains all of the events.

    It stores them in a dictionary of the following format:
    <cf-name>: Dictionary of cf events
    (The db-wide events are stored under the "cf name" No_COL_NAME)

    Dictionary of cf events is itself a dictionary of the following format:
    <event-type>: List of Event-s, ordered by their time
    """
    def __init__(self, job_id_to_cf_name_map):
        assert isinstance(job_id_to_cf_name_map, dict)

        self.job_id_to_cf_name_map = job_id_to_cf_name_map
        self.preambles = dict()
        self.events = dict()

    def try_parsing_as_preamble(self, entry):
        preamble_info = Event.try_parse_as_preamble(entry)
        if not preamble_info:
            return None
        assert isinstance(preamble_info, Event.EventPreambleInfo)

        # If a preamble was already encountered, it must be for the same
        # parameters
        job_id = preamble_info.job_id
        if preamble_info.job_id not in self.preambles:
            self.preambles[job_id] = preamble_info
        else:
            if not self.preambles[job_id].are_equal_ignoring_wal_id(
                    preamble_info):
                logging.error(
                    f"A preamble with same job id exists, but other preamble"
                    f" info mismatching (IGNORING NEW). "
                    f"Existing:{self.preambles[job_id]}. New:{preamble_info}")
                # Indicating as a preamble, but returning the existing one
                return self.preambles[job_id]

        return preamble_info

    def try_adding_entry(self, entry):
        assert isinstance(entry, LogEntry)

        # A preamble event is an entry that will be pending for its
        # associated event entry to provide the event with its cf name
        preamble_info = self.try_parsing_as_preamble(entry)
        if preamble_info:
            return True, None, preamble_info.cf_name

        if not Event.is_an_event_entry(entry):
            return False, None, None

        try:
            event = Event.create_event(entry)
        except utils.ParsingError as e:
            # telling caller I have added it as an event since it's
            # supposedly an event, but badly formatted somehow
            logging.error(f"Discarding badly constructed event.\n{entry} "
                          f"(exception: {e})")
            return True, None, None

        # Combine associated event preamble, if any exists
        event_job_id = event.get_job_id()

        # The preamble may provide the cf_name and possible other info
        if event_job_id in self.preambles:
            preamble_info = self.preambles[event_job_id]
            if event.try_adding_preamble_event(preamble_info):
                del(self.preambles[event_job_id])

        if event.is_db_wide_event():
            self.__try_finding_cf_for_newly_added_event(event_job_id, event)

        cf_name = event.get_cf_name()
        try:
            self.__add_event(event_job_id, cf_name, event.get_type(), event)
        except utils.ParsingError:
            logging.error(f"Error adding an event.Discarding it. \n"
                          f"event:{event}")
            return True, None, None

        self.__try_to_match_newly_added_event(event)

        if cf_name == utils.NO_CF:
            cf_name = None
        return True, event, cf_name

    def __try_finding_cf_for_newly_added_event(self, event_job_id, event):
        assert event.is_db_wide_event()

        if event_job_id not in self.events:
            return None

        # Existing (earlier) events with the same job id should have
        # their cf name set. Try to find one and use it
        # Assuming a job id is unique to a cf
        job_cfs_names = [cf_name for cf_name in
                         self.events[event_job_id].keys() if cf_name !=
                         utils.NO_CF]
        if not job_cfs_names:
            return None

        assert len(job_cfs_names) == 1
        cf_name = job_cfs_names[0]
        event.set_cf_name(cf_name)

        return cf_name

    def __add_event(self, job_id, cf_name, event_type, event):
        if job_id not in self.events:
            self.events[job_id] = dict()
        job_events = self.events[job_id]
        EventsMngr.__validate_job_has_cf_or_no_other(job_events, cf_name,
                                                     event)

        if cf_name not in job_events:
            job_events[cf_name] = dict()
        if event_type not in job_events[cf_name]:
            job_events[cf_name][event_type] = []
        job_events[cf_name][event_type].append(event)

    @staticmethod
    def __validate_job_has_cf_or_no_other(job_events, cf_name, event):
        # It is illegal to have a job id with events in multiple cf-s.
        # The only other allowed "cf" is the no-col-family cf
        if not job_events or \
                cf_name == utils.NO_CF or \
                cf_name in job_events:
            return

        # The only valid option is that job_events will only have
        # the NO_COL_FAMILY or be empty (checked above)
        job_cf_names = list(job_events.keys())
        if job_cf_names != [utils.NO_CF]:
            raise utils.ParsingError(
                f"Job has events for more than one cf. "
                f"CF-s: ({job_cf_names}). cf_name:{cf_name}, "
                f"\nEvent:{event}")

    def __try_to_match_newly_added_event(self, new_event):
        cf_name = new_event.get_cf_name()
        new_event_type = new_event.get_type()

        matching_event_type_info = \
            Event.get_matching_type_info_if_exists(new_event_type)
        if not matching_event_type_info:
            return
        if not matching_event_type_info.is_start:
            # Attempt to match only the end event (the matching will be the
            # start)
            return

        potentially_matching_events =\
            self.get_cf_events_by_type(cf_name,
                                       matching_event_type_info.event_type)
        # Try to find a match from the most recent to the first
        for potential_event in reversed(potentially_matching_events):
            if new_event.try_adding_matching_event(potential_event):
                break

    def get_cf_events(self, cf_name):
        all_cf_events = []

        for job_events in self.events.values():
            if cf_name not in job_events:
                continue
            for cf_events in job_events[cf_name].values():
                all_cf_events.extend(list(cf_events))

        # Return the events sorted by their time
        all_cf_events.sort()
        return all_cf_events

    def get_cf_events_by_type(self, cf_name, event_type):
        assert isinstance(event_type, EventType)

        all_cf_events = []

        for job_events in self.events.values():
            if cf_name not in job_events:
                continue
            if event_type not in job_events[cf_name]:
                continue
            all_cf_events.extend(job_events[cf_name][event_type])

        # The list may not be ordered due to the original time issue
        # or having event preambles matched to their events somehow
        # out of order. Sorting will insure correctness even if the list
        # is already sorted
        all_cf_events.sort()
        return all_cf_events

    def get_cf_flow_events(self, flow_type, cf_name):
        flow_events = []

        event_start_type = get_flow_start_event_type(flow_type)

        starting_events = self.get_cf_events_by_type(cf_name,
                                                     event_start_type)
        for start_event in starting_events:
            end_event = None
            matching_end_event_info = \
                start_event.get_matching_event_info_if_exists()
            if matching_end_event_info:
                end_event = matching_end_event_info.event
            flow_events.append((start_event, end_event))

        return flow_events

    def get_all_flow_events(self, flow_type, cfs_names):
        # The cf_name is in the event data => no need to have a per-cf
        # dictionary and we will sort the events based on their time
        flow_events = []

        for cf_name in cfs_names:
            cf_flow_events = self.get_cf_flow_events(flow_type, cf_name)
            if cf_flow_events:
                flow_events.extend(cf_flow_events)

        # Sort the events based on the start event (
        flow_events.sort(key=lambda a: a[0])

        return flow_events

    def debug_get_all_events(self):
        return self.events
