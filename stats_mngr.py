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
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto

import regexes
import utils

format_err_msg = utils.format_err_msg
ParsingAssertion = utils.ParsingAssertion
ErrContext = utils.ErrorContext
format_line_num_from_entry = utils.format_line_num_from_entry
format_line_num_from_line_idx = utils.format_line_num_from_line_idx
get_line_num_from_entry = utils.get_line_num_from_entry


def is_empty_line(line):
    return re.fullmatch(regexes.EMPTY_LINE, line) is not None


def parse_uptime_line(line, allow_mismatch=False):
    # Uptime(secs): 603.0 total, 600.0 interval
    match = re.search(regexes.UPTIME_STATS_LINE, line)

    if not match:
        if allow_mismatch:
            return None
        if not match:
            raise ParsingAssertion("Failed parsing uptime line",
                                   ErrContext(**{"line": line}))

    total_sec = float(match.group('total'))
    interval_sec = float(match.group('interval'))
    return total_sec, interval_sec


def parse_line_with_cf(line, regex_str, allow_mismatch=False):
    line_parts = re.findall(regex_str, line)

    if not line_parts:
        if allow_mismatch:
            return None
        if not line_parts:
            raise ParsingAssertion("Failed parsing line with column-family",
                                   ErrContext(**{"line": line}))

    cf_name = line_parts[0]
    return cf_name


class DbWideStatsMngr:
    """ Parses and stores dumps of the db-wide stats at the top of the
     DUMPING STATS dump
    """
    @dataclass
    class CumulativeWritesInfo:
        num_writes: int = 0
        num_keys: int = 0
        ingest: int = 0
        ingest_rate_mbps: float = 0.0

    @staticmethod
    def is_start_line(line):
        return re.fullmatch(regexes.DB_STATS, line) is not None

    def __init__(self):
        self.stalls = {}
        self.cumulative_writes = {}

    def add_lines(self, time, db_stats_lines):
        assert len(db_stats_lines) > 0

        self.stalls[time] = {}

        for line in db_stats_lines[1:]:
            if self.try_parse_as_interval_stall_line(time, line):
                continue
            elif self.try_parse_as_cumulative_stall_line(time, line):
                continue
            elif self.try_parse_as_cumulative_writes_line(time, line):
                continue

        if DbWideStatsMngr.is_all_zeroes_entry(self.stalls[time]):
            del self.stalls[time]

    @staticmethod
    def try_parse_as_stalls_line(regex, line):
        line_parts = re.findall(regex, line)
        if not line_parts:
            return None

        assert len(line_parts) == 1 and len(line_parts[0]) == 5

        hours, minutes, seconds, ms, stall_percent = line_parts[0]
        stall_duration = timedelta(hours=int(hours),
                                   minutes=int(minutes),
                                   seconds=int(seconds),
                                   milliseconds=int(ms))
        return stall_duration, stall_percent

    def try_parse_as_interval_stall_line(self, time, line):
        stall_info = DbWideStatsMngr.try_parse_as_stalls_line(
            regexes.DB_WIDE_INTERVAL_STALL, line)
        if stall_info is None:
            return None

        stall_duration, stall_percent = stall_info
        self.stalls[time].update({"interval_duration": stall_duration,
                                  "interval_percent": float(stall_percent)})

    def try_parse_as_cumulative_stall_line(self, time, line):
        stall_info = DbWideStatsMngr.try_parse_as_stalls_line(
            regexes.DB_WIDE_CUMULATIVE_STALL, line)
        if stall_info is None:
            return None

        stall_duration, stall_percent = stall_info
        self.stalls[time].update({"cumulative_duration": stall_duration,
                                  "cumulative_percent": float(stall_percent)})

    def try_parse_as_cumulative_writes_line(self, time, line):
        writes_info = re.findall(regexes.DB_WIDE_CUMULATIVE_WRITES, line)
        if not writes_info:
            return None
        assert len(writes_info) == 1 and len(writes_info[0]) == 6

        writes_info = writes_info[0]
        info = DbWideStatsMngr.CumulativeWritesInfo()
        info.num_writes = utils.get_number_from_human_readable_components(
            writes_info[0], writes_info[1])
        info.num_keys = utils.get_number_from_human_readable_components(
            writes_info[2], writes_info[3])
        # Although ingest is a counter it's printed in units of GB (bytes)
        info.ingest = \
            utils.get_num_bytes_from_human_readable_str(f"{writes_info[4]} GB")
        # Keep the ingest rate in MBPS as it is printed
        info.ingest_rate_mbps = float(writes_info[5])

        self.cumulative_writes[time] = info

    @staticmethod
    def is_all_zeroes_entry(entry):
        interval_duration_total_secs = 0.0
        interval_percent = 0.0
        cumulative_duration_total_secs = 0
        cumulative_percent = 0.0

        if "interval_duration" in entry:
            interval_duration_total_secs = \
                entry["interval_duration"].total_seconds()
        if "interval_percent" in entry:
            interval_percent = entry["interval_percent"]
        if "cumulative_duration" in entry:
            cumulative_duration_total_secs = \
                entry["cumulative_duration"].total_seconds()
        if "cumulative_percent" in entry:
            interval_percent = entry["cumulative_percent"]

        return interval_duration_total_secs == 0.0 and \
            interval_percent == 0.0 and \
            cumulative_duration_total_secs == 0.0 and \
            cumulative_percent == 0.0

    def get_stalls_entries(self):
        return self.stalls

    def get_cumulative_writes_entries(self):
        return self.cumulative_writes

    def get_last_cumulative_writes_entry(self):
        if not self.cumulative_writes:
            return None
        return utils.get_last_dict_entry(self.cumulative_writes)


class CompactionStatsMngr:
    class LineType(Enum):
        LEVEL = auto()
        SUM = auto()
        INTERVAL = auto()
        USER = auto()

    class LevelFields(str, Enum):
        SIZE_BYTES = 'size_bytes'
        WRITE_AMP = 'W-Amp'
        COMP_SEC = 'Comp(sec)'
        COMP_MERGE_CPU = 'CompMergeCPU(sec)'

        @staticmethod
        def get_fields_list():
            return list(CompactionStatsMngr.LevelFields.__members__)

        @staticmethod
        def has_field(field):
            assert isinstance(field, CompactionStatsMngr.LevelFields)
            return field.name in \
                CompactionStatsMngr.LevelFields.get_fields_list()

    class CfLevelEntry:
        def __init__(self, entry):
            self.time = list(entry.keys())[0]
            self.lines = entry[self.time]

        @staticmethod
        def get_level_key(level):
            return f"LEVEL-{level}"

        def get_time(self):
            return self.time

        def get_levels(self):
            level_regex = fr"LEVEL-{regexes.INT_C}"
            levels = []
            for key in self.lines.keys():
                match = re.findall(level_regex, key)
                if match:
                    levels.append(int(match[0]))
            return levels

        def get_level_line(self, level):
            level_key = __class__.get_level_key(level)
            if level_key not in self.lines:
                return None
            return self.lines[level_key]

        def get_sum_line(self):
            return self.lines['SUM']

        def get_total_size_bytes(self):
            return

    @staticmethod
    def parse_start_line(line, allow_mismatch=False):
        return parse_line_with_cf(line, regexes.COMPACTION_STATS,
                                  allow_mismatch)

    @staticmethod
    def is_start_line(line):
        return CompactionStatsMngr.parse_start_line(line,
                                                    allow_mismatch=True) \
               is not None

    def __init__(self):
        self.level_entries = dict()
        self.priority_entries = dict()

    def add_lines(self, time, cf_name, stats_lines):
        stats_lines = [line.strip() for line in stats_lines]
        assert cf_name == \
               CompactionStatsMngr.parse_start_line(stats_lines[0])

        if stats_lines[1].startswith('Level'):
            self.parse_level_lines(time, cf_name, stats_lines[1:])
        elif stats_lines[1].startswith('Priority'):
            self.parse_priority_lines(time, cf_name, stats_lines[1:])
        else:
            assert 0

    @staticmethod
    def parse_header_line(header_line, separator_line):
        # separator line is expected to be all "-"-s
        if set(separator_line.strip()) != {"-"}:
            # TODO - Issue an error / warning
            return None

        header_fields = header_line.split()

        if header_fields[0] != 'Level' or header_fields[1] != "Files" or \
                header_fields[2] != "Size":
            # TODO - Issue an error / warning
            return None

        return header_fields

    @staticmethod
    def determine_line_type(type_field_str):
        type_field_str = type_field_str.strip()
        level_num = None
        line_type = None
        if type_field_str == "Sum":
            line_type = CompactionStatsMngr.LineType.SUM
        elif type_field_str == "Int":
            line_type = CompactionStatsMngr.LineType.INTERVAL
        elif type_field_str == "User":
            line_type = CompactionStatsMngr.LineType.USER
        else:
            level_match = re.findall(r"L(\d+)", type_field_str)
            if level_match:
                line_type = CompactionStatsMngr.LineType.LEVEL
                level_num = int(level_match[0])
            else:
                # TODO - Error
                pass

        return line_type, level_num

    @staticmethod
    def parse_files_field(files_field):
        files_parts = re.findall(r"(\d+)/(\d+)", files_field)
        if not files_parts:
            # TODO - Error
            return None

        return files_parts[0][0], files_parts[0][1]

    @staticmethod
    def parse_size_field(size_value, size_units):
        return utils.get_num_bytes_from_human_readable_components(
            size_value,
            size_units)

    def parse_level_lines(self, time, cf_name, stats_lines):
        header_fields = CompactionStatsMngr.parse_header_line(stats_lines[0],
                                                              stats_lines[1])
        if header_fields is None:
            # TODO - Error?
            return

        new_entry = {}
        for line in stats_lines[2:]:
            line_fields = line.strip().split()
            if not line_fields:
                continue
            line_type, level_num = \
                CompactionStatsMngr.determine_line_type(line_fields[0])
            if line_type is None:
                # TODO - Error
                return

            num_files, files_in_comp = \
                CompactionStatsMngr.parse_files_field(line_fields[1])
            if files_in_comp is None:
                # TODO - Error
                return

            size_in_units = line_fields[2]
            size_units = line_fields[3]

            key = line_type.name
            if line_type is CompactionStatsMngr.LineType.LEVEL:
                key += f"-{level_num}"

            new_entry[key] = {
                "Num-Files": num_files,
                "Files-In-Comp": files_in_comp,
                "size_bytes":
                    CompactionStatsMngr.parse_size_field(size_in_units,
                                                         size_units)
            }
            # A valid line must have one more field than in the header line
            if len(line_fields) != len(header_fields) + 1:
                logging.error(
                    f"Expected #{len(header_fields)+1} fields in line, "
                    f"when there are {len(line_fields)} in compaction level "
                    f"stats. time:{time}, cf:{cf_name}.\n"
                    f"line:{line}")
                return

            new_entry[key].update({
                header_fields[i]: line_fields[i+1]
                for i in range(3, len(header_fields))
            })

        if CompactionStatsMngr.LineType.SUM.name not in new_entry:
            logging.error(
                f"Error parsing compaction stats level lines. "
                f"time:{time}, cf:{cf_name}.\n"
                f"stats lines:{stats_lines}")
            return

        if time not in self.level_entries:
            self.level_entries[time] = {}

        self.level_entries[time][cf_name] = new_entry

    def parse_priority_lines(self, time, cf_name, stats_lines):
        # TODO - Consider issuing an info message as Redis (e.g.) don not
        #  have any content here
        if len(stats_lines) < 4:
            return

        # TODO: Parse when doing something with the data
        pass

    def get_level_entries(self):
        return self.level_entries

    def get_cf_level_entries(self, cf_name):
        cf_entries = []
        for time, time_entries in self.level_entries.items():
            if cf_name in time_entries:
                cf_entries.append({time: time_entries[cf_name]})

        return cf_entries

    def get_first_cf_level_entry(self, cf_name):
        all_entries = self.get_cf_level_entries(cf_name)
        if not all_entries:
            return None
        return all_entries[0]

    def get_last_cf_level_entry(self, cf_name):
        all_entries = self.get_cf_level_entries(cf_name)
        if not all_entries:
            return None
        return all_entries[-1]

    def get_first_level_entry_all_cfs(self):
        all_entries = self.get_level_entries()
        if not all_entries:
            return None

        time, first_all_cfs_dumps = \
            utils.get_first_dict_entry_components(all_entries)
        return {cf_name: {time: cf_entry} for cf_name, cf_entry in
                first_all_cfs_dumps.items()}

    def get_last_level_entry_all_cfs(self):
        all_entries = self.get_level_entries()
        if not all_entries:
            return None

        time, last_all_cfs_dumps = \
            utils.get_last_dict_entry_components(all_entries)
        return {cf_name: {time: cf_entry} for cf_name, cf_entry in
                last_all_cfs_dumps.items()}

    @staticmethod
    def get_time_of_entry(stats_table):
        keys = list(stats_table.keys())
        assert len(keys) == 1
        return keys[0]

    @staticmethod
    def get_level_entry_uptime_seconds(entry, log_start_time):
        entry_time = CompactionStatsMngr.get_time_of_entry(entry)
        return utils.get_times_strs_diff_seconds(log_start_time, entry_time)

    @staticmethod
    def get_field_value_for_line_in_entry(stats_table, line_key, field):
        assert CompactionStatsMngr.LevelFields.has_field(field)

        time = CompactionStatsMngr.get_time_of_entry(stats_table)
        if line_key not in list(stats_table[time].keys()):
            logging.info(f"{line_key} not in entry. time:{time}")
            return None

        level_line = stats_table[time][line_key]
        if field.value not in level_line:
            logging.info(f"{field.value} not in entry's line. time:{time}\n"
                         f"{level_line}")
            return None

        return level_line[field.value]

    @staticmethod
    def get_field_value_for_all_levels(stats_table, field):
        assert CompactionStatsMngr.LevelFields.has_field(field)

        per_level_value = {}
        time = CompactionStatsMngr.get_time_of_entry(stats_table)

        line_key_regex = fr"LEVEL-{regexes.INT_C}"
        for key, value in stats_table[time].items():
            match = re.findall(line_key_regex, key)
            if not match:
                continue

            level = int(match[0])
            per_level_value[level] = \
                CompactionStatsMngr.get_field_value_for_line_in_entry(
                    stats_table, key, field)

        if not per_level_value:
            return None
        return per_level_value

    @staticmethod
    def get_level_field_value(stats_table, level, field):
        per_level_value = \
            CompactionStatsMngr.get_field_value_for_all_levels(
                stats_table, field)

        if per_level_value is None:
            return None

        if level not in per_level_value:
            logging.info(f"No info for level {level} in table.")
            return None

        return per_level_value[level]

    @staticmethod
    def get_sum_value(stats_table, field):
        line_key = CompactionStatsMngr.LineType.SUM.name
        value = \
            CompactionStatsMngr.get_field_value_for_line_in_entry(
                stats_table, line_key, field)

        if value is None:
            # SUM must be present always
            raise utils.ParsingError(
                f"{line_key} is missing from compaction stats.\n{stats_table}")

        return value

    def get_cf_size_bytes_at_end(self, cf_name):
        last_cf_entry = self.get_last_cf_level_entry(cf_name)
        if not last_cf_entry:
            return None
        return CompactionStatsMngr.get_field_value_for_line_in_entry(
            last_cf_entry, CompactionStatsMngr.LineType.SUM.name,
            CompactionStatsMngr.LevelFields.SIZE_BYTES)

    def get_cf_level_size_bytes(self, cf_name, level):
        last_cf_entry = self.get_last_cf_level_entry(cf_name)
        if not last_cf_entry:
            return None
        # TODO - Turn this into a utility
        level_key = f"LEVEL-{level}"
        return CompactionStatsMngr.get_field_value_for_line_in_entry(
            last_cf_entry, level_key,
            CompactionStatsMngr.LevelFields.SIZE_BYTES)


class BlobStatsMngr:
    @staticmethod
    def parse_blob_stats_line(line, allow_mismatch=False):
        line_parts = re.findall(regexes.BLOB_STATS_LINE, line)
        if not line_parts:
            if allow_mismatch:
                return None
            assert line_parts

        file_count, total_size_gb, garbage_size_gb, space_amp = line_parts[0]
        return \
            int(file_count), float(total_size_gb), float(garbage_size_gb), \
            float(space_amp)

    @staticmethod
    def is_start_line(line):
        return \
            BlobStatsMngr.parse_blob_stats_line(line, allow_mismatch=True) \
            is not None

    def __init__(self):
        self.entries = dict()

    def add_lines(self, time, cf_name, db_stats_lines):
        assert len(db_stats_lines) > 0
        line = db_stats_lines[0]

        line_parts = re.findall(regexes.BLOB_STATS_LINE, line)
        assert line_parts and len(line_parts) == 1 and len(line_parts[0]) == 4

        components = line_parts[0]
        file_count = int(components[0])
        total_size_bytes = \
            utils.get_num_bytes_from_human_readable_components(
                components[1],
                "GB")
        garbage_size_bytes = \
            utils.get_num_bytes_from_human_readable_components(
                components[2],
                "GB")
        space_amp = float(components[3])

        if cf_name not in self.entries:
            self.entries[cf_name] = dict()
        self.entries[cf_name][time] = {
            "File Count": file_count,
            "Total Size": total_size_bytes,
            "Garbage Size": garbage_size_bytes,
            "Space Amp": space_amp
        }

    def get_cf_stats(self, cf_name):
        if cf_name not in self.entries:
            return []
        return self.entries[cf_name]


class CfNoFileStatsMngr:
    @staticmethod
    def is_start_line(line):
        return parse_uptime_line(line, allow_mismatch=True)

    def __init__(self):
        self.stall_counts = {}

    def try_parse_as_stalls_count_line(self, time, cf_name, line):
        if not line.startswith(regexes.CF_STALLS_LINE_START):
            return None

        if cf_name not in self.stall_counts:
            self.stall_counts[cf_name] = {}
        # TODO - I have seen compaction stats for the same cf twice - WHY?
        #######assert time not in self.stall_counts[cf_name] # noqa
        self.stall_counts[cf_name][time] = {}

        stall_count_and_reason_matches = \
            re.compile(regexes.CF_STALLS_COUNT_AND_REASON)
        sum_fields_count = 0
        for match in stall_count_and_reason_matches.finditer(line):
            count = int(match[1])
            self.stall_counts[cf_name][time][match[2]] = count
            sum_fields_count += count
        assert self.stall_counts[cf_name][time]

        total_count_match = re.findall(
            regexes.CF_STALLS_INTERVAL_COUNT, line)

        # TODO - Last line of Redis's log was cropped in the middle
        ###### assert total_count_match and len(total_count_match) == 1 # noqa
        if not total_count_match or len(total_count_match) != 1:
            del self.stall_counts[cf_name][time]
            return None

        total_count = int(total_count_match[0])
        self.stall_counts[cf_name][time]["interval_total_count"] = total_count
        sum_fields_count += total_count

        if sum_fields_count == 0:
            del self.stall_counts[cf_name][time]

    def add_lines(self, time, cf_name, stats_lines):
        for line in stats_lines:
            line = line.strip()
            if self.try_parse_as_stalls_count_line(time, cf_name, line):
                continue

    def get_stall_counts(self):
        return self.stall_counts


class CfFileHistogramStatsMngr:
    @dataclass
    class CfLevelStats:
        count: int = 0
        average: float = 0.0
        std_dev: float = 0.0
        min: int = 0
        median: float = 0.0
        max: int = 0

    class CfEntry:
        def __init__(self, entry):
            self.time = __class__.get_time(entry)
            self.levels_stats = entry[self.time]

        @staticmethod
        def get_time(entry):
            return list(entry.keys())[0]

        def get_levels(self):
            return list(self.levels_stats.keys())

        def get_all_levels_stats(self):
            return self.levels_stats

        def get_level_stats(self, level):
            if level not in self.levels_stats:
                return None
            return self.levels_stats[level]

    @staticmethod
    def parse_start_line(line, allow_mismatch=False):
        return parse_line_with_cf(line,
                                  regexes.FILE_READ_LATENCY_STATS,
                                  allow_mismatch)

    @staticmethod
    def is_start_line(line):
        return CfFileHistogramStatsMngr.parse_start_line(line,
                                                         allow_mismatch=True) \
               is not None

    def __init__(self):
        # Format: {<cf name>: {time: {<level>: CfLevelStats}}}
        self.stats = dict()

    def add_lines(self, time, cf_name, db_stats_lines):
        # The lines are organized as follows:
        # <cf-1> Header
        # Level-<X> stats
        # Level-<Y> stats
        # ...
        # A cf header is:
        # ** File Read Latency Histogram By Level [<cf-name>] **
        #
        # The per level dumps are:
        #  ** Level 0 read latency histogram (<cf-name>):
        # Count: 25 Average: 1571.7200  StdDev: 5194.93
        # Min: 1  Median: 2.8333  Max: 26097
        # Percentiles: <Percentiles Line> (Not parsed currently)
        # Per-Bucket Histogram Table (Not parsed currently)
        #
        assert isinstance(db_stats_lines, list)

        num_lines = len(db_stats_lines)
        assert num_lines > 0

        # First line must be the start of a cf section
        parsed_cf_name = CfFileHistogramStatsMngr.parse_start_line(
            db_stats_lines[0])
        assert cf_name == parsed_cf_name, \
            f"cf_name:{cf_name}, parsed_cf_name:{parsed_cf_name}"

        line_idx = 1
        while line_idx < num_lines:
            next_line_idx = \
                CfFileHistogramStatsMngr.find_next_level_stats_start(
                    db_stats_lines, line_idx + 1)
            self.parse_next_level_stats(
                time, cf_name, db_stats_lines[line_idx:next_line_idx])
            line_idx = next_line_idx

    @staticmethod
    def find_next_level_stats_start(db_stats_lines, line_idx):
        while line_idx < len(db_stats_lines):
            if CfFileHistogramStatsMngr.\
                    is_level_stats_start_line(db_stats_lines[line_idx]):
                break
            line_idx += 1

        return line_idx

    def parse_next_level_stats(self, time, cf_name, level_stats_lines):
        if len(level_stats_lines) < 3:
            raise utils.ParsingError(
                f"Expecting at least 3 lines. time:{time}\n"
                f"{level_stats_lines}")

        new_stats = CfFileHistogramStatsMngr.CfLevelStats()
        level = CfFileHistogramStatsMngr.parse_level_line(
            time, cf_name, level_stats_lines[0])

        CfFileHistogramStatsMngr.parse_stats_line_1(
            time, cf_name, level_stats_lines[1], new_stats)
        CfFileHistogramStatsMngr.parse_stats_line_2(
            time, cf_name, level_stats_lines[2], new_stats)

        self.add_cf_if_necessary(cf_name)
        self.add_cf_time_if_necessary(cf_name, time)
        if level in self.stats[cf_name][time]:
            logging.warning(
                f"Duplicate file read latency for level, Ignoring. "
                f"tims:{time}, cf:{cf_name}. level:{level}\n"
                f"{level_stats_lines}")
            return

        self.stats[cf_name][time][level] = new_stats

    def add_cf_if_necessary(self, cf_name):
        if cf_name not in self.stats:
            self.stats[cf_name] = {}

    def add_cf_time_if_necessary(self, cf_name, time):
        if time not in self.stats[cf_name]:
            self.stats[cf_name][time] = {}

    @staticmethod
    def parse_level_line(time, cf_name, line):
        match = re.findall(regexes.LEVEL_READ_LATENCY_LEVEL_LINE, line)
        if not match:
            raise utils.ParsingError(
                f"Failed parsing read latency level line. "
                f"time:{time}, cf:{cf_name}\n"
                f"{line}")
        assert len(match) == 1

        # Return the level
        return int(match[0])

    @staticmethod
    def is_level_stats_start_line(line):
        match = re.findall(regexes.LEVEL_READ_LATENCY_LEVEL_LINE, line)
        return True if match else False

    @staticmethod
    def parse_stats_line_1(time, cf_name, line, new_stats):
        match = re.findall(regexes.LEVEL_READ_LATENCY_STATS_LINE1, line)
        if not match:
            raise utils.ParsingError(
                f"Failed parsing read latency level line. "
                f"time:{time}, cf:{cf_name}\n"
                f"{line}")
        assert len(match) == 1 or len(match[0]) == 3

        new_stats.count = int(match[0][0])
        new_stats.average = float(match[0][1])
        new_stats.std_dev = float(match[0][2])

    @staticmethod
    def parse_stats_line_2(time, cf_name, line, new_stats):
        match = re.findall(regexes.LEVEL_READ_LATENCY_STATS_LINE2, line)
        if not match:
            raise utils.ParsingError(
                f"Failed parsing read latency level line. "
                f"time:{time}, cf:{cf_name}\n"
                f"{line}")
        assert len(match) == 1 or len(match[0]) == 3

        new_stats.min = int(match[0][0])
        new_stats.median = float(match[0][1])
        new_stats.max = int(match[0][2])

    def get_all_entries(self):
        if not self.stats:
            return None

        return self.stats

    def get_cf_entries(self, cf_name):
        if cf_name not in self.stats:
            return None

        return self.stats[cf_name]

    def get_last_cf_entry(self, cf_name):
        all_entries = self.get_cf_entries(cf_name)
        if not all_entries:
            return None
        return utils.get_last_dict_entry(all_entries)


class BlockCacheStatsMngr:
    @staticmethod
    def is_start_line(line):
        return re.findall(regexes.BLOCK_CACHE_STATS_START, line)

    def __init__(self):
        self.caches = dict()

    def add_lines(self, time, cf_name, db_stats_lines):
        if len(db_stats_lines) < 2:
            return

        cache_id = self.parse_cache_id_line(db_stats_lines[0])
        self.parse_global_entry_stats_line(time, cache_id, db_stats_lines[1])
        if len(db_stats_lines) > 2:
            self.parse_cf_entry_stats_line(time, cache_id, db_stats_lines[2])
        return cache_id

    def parse_cache_id_line(self, line):
        line_parts = re.findall(regexes.BLOCK_CACHE_STATS_START, line)
        assert line_parts and len(line_parts) == 1 and len(line_parts[0]) == 3
        cache_id, cache_capacity, capacity_units = line_parts[0]
        capacity_bytes = utils.\
            get_num_bytes_from_human_readable_components(cache_capacity,
                                                         capacity_units)

        if cache_id not in self.caches:
            self.caches[cache_id] = {"Capacity": capacity_bytes,
                                     "Usage": 0}

        return cache_id

    def parse_global_entry_stats_line(self, time, cache_id, line):
        line_parts = re.findall(regexes.BLOCK_CACHE_ENTRY_STATS, line)
        assert line_parts and len(line_parts) == 1

        roles, roles_stats = BlockCacheStatsMngr.parse_entry_stats_line(
            line_parts[0])

        self.add_time_if_necessary(cache_id, time)
        self.caches[cache_id][time]["Usage"] = 0

        usage = 0
        for i, role in enumerate(roles):
            count, size_with_unit, portion = roles_stats[i].split(',')
            size_bytes = \
                utils.get_num_bytes_from_human_readable_str(
                    size_with_unit)
            portion = f"{float(portion.split('%')[0]):.2f}%"
            self.caches[cache_id][time][role] = \
                {"Count": int(count), "Size": size_bytes, "Portion": portion}
            usage += size_bytes

        self.caches[cache_id][time]["Usage"] = usage
        self.caches[cache_id]["Usage"] = usage

    def parse_cf_entry_stats_line(self, time, cache_id, line):
        line_parts = re.findall(regexes.BLOCK_CACHE_CF_ENTRY_STATS, line)
        if not line_parts:
            return
        assert len(line_parts) == 1 and len(line_parts[0]) == 2

        cf_name, roles_info_part = line_parts[0]

        roles, roles_stats = BlockCacheStatsMngr.parse_entry_stats_line(
            roles_info_part)

        cf_entry = {}
        for i, role in enumerate(roles):
            size_bytes = \
                utils.get_num_bytes_from_human_readable_str(
                    roles_stats[i])
            if size_bytes > 0:
                cf_entry[role] = size_bytes

        if cf_entry:
            if "CF-s" not in self.caches[cache_id][time]:
                self.add_time_if_necessary(cache_id, time)
                self.caches[cache_id][time]["CF-s"] = {}
            self.caches[cache_id][time]["CF-s"][cf_name] = cf_entry

    @staticmethod
    def parse_entry_stats_line(line):
        roles = re.findall(regexes.BLOCK_CACHE_ENTRY_ROLES_NAMES,
                           line)
        roles_stats = re.findall(regexes.BLOCK_CACHE_ENTRY_ROLES_STATS,
                                 line)
        if len(roles) != len(roles_stats):
            assert False, str(ParsingAssertion(
                f"Error Parsing block cache stats line. "
                f"roles:{roles}, roles_stats:{roles_stats}",
                ErrContext(**{'log_line': line})))

        return roles, roles_stats

    def add_time_if_necessary(self, cache_id, time):
        if time not in self.caches[cache_id]:
            self.caches[cache_id][time] = {}

    def get_cache_entries(self, cache_id):
        if cache_id not in self.caches:
            return {}
        return self.caches[cache_id]

    def get_cf_cache_entries(self, cache_id, cf_name):
        cf_entries = {}

        all_cache_entries = self.get_cache_entries(cache_id)
        if not all_cache_entries:
            return cf_entries

        cf_entries = {}
        for key in all_cache_entries.keys():
            time = utils.parse_time_str(key, expect_valid_str=False)
            if time:
                time = key
                if "CF-s" in all_cache_entries[time]:
                    if cf_name in all_cache_entries[time]["CF-s"]:
                        cf_entries[time] = \
                            all_cache_entries[time]["CF-s"][cf_name]

        return cf_entries

    def get_all_cache_entries(self):
        return self.caches

    def get_last_usage(self, cache_id):
        usage = 0
        if self.caches:
            usage = self.caches[cache_id]["Usage"]
        return usage


class StatsMngr:
    class StatsType(Enum):
        DB_WIDE = auto()
        COMPACTION = auto()
        BLOB = auto()
        BLOCK_CACHE = auto()
        CF_NO_FILE = auto()
        CF_FILE_HISTOGRAM = auto()
        COUNTERS = auto()

    def __init__(self):
        self.db_wide_stats_mngr = DbWideStatsMngr()
        self.compaction_stats_mngr = CompactionStatsMngr()
        self.blob_stats_mngr = BlobStatsMngr()
        self.block_cache_stats_mngr = BlockCacheStatsMngr()
        self.cf_no_file_stats_mngr = CfNoFileStatsMngr()
        self.cf_file_histogram_stats_mngr = CfFileHistogramStatsMngr()

    @staticmethod
    def is_dump_stats_start(entry):
        return entry.get_msg().startswith(regexes.DUMP_STATS_STR)

    @staticmethod
    def find_next_start_line_in_db_stats(db_stats_lines,
                                         start_line_idx,
                                         curr_stats_type):
        line_idx = start_line_idx + 1
        next_stats_type = None
        cf_name = None
        # DB Wide Stats must be the first and were verified above
        while line_idx < len(db_stats_lines) and next_stats_type is None:
            line = db_stats_lines[line_idx]

            if CompactionStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.COMPACTION
                cf_name = CompactionStatsMngr.parse_start_line(line)
            elif BlobStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.BLOB
            elif BlockCacheStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.BLOCK_CACHE
            elif CfFileHistogramStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.CF_FILE_HISTOGRAM
                cf_name = CfFileHistogramStatsMngr.parse_start_line(line)
            elif CfNoFileStatsMngr.is_start_line(line) and \
                    curr_stats_type != StatsMngr.StatsType.DB_WIDE:
                next_stats_type = StatsMngr.StatsType.CF_NO_FILE
            else:
                line_idx += 1

        return line_idx, next_stats_type, cf_name

    def parse_next_db_stats_entry_lines(self, time, cf_name, stats_type,
                                        entry_start_line_num,
                                        db_stats_lines, start_line_idx,
                                        end_line_idx):
        assert end_line_idx <= len(db_stats_lines)
        stats_lines_to_parse = db_stats_lines[start_line_idx:end_line_idx]
        stats_lines_to_parse = [line.strip() for line in stats_lines_to_parse]

        try:
            logging.debug(
                f"Parsing Stats Component ({stats_type.name}) "
                f"[line# {entry_start_line_num + start_line_idx + 1}]")

            valid_stats_type = True
            if stats_type == StatsMngr.StatsType.DB_WIDE:
                self.db_wide_stats_mngr.add_lines(time, stats_lines_to_parse)
            elif stats_type == StatsMngr.StatsType.COMPACTION:
                self.compaction_stats_mngr.add_lines(time, cf_name,
                                                     stats_lines_to_parse)
            elif stats_type == StatsMngr.StatsType.BLOB:
                self.blob_stats_mngr.add_lines(time, cf_name,
                                               stats_lines_to_parse)
            elif stats_type == StatsMngr.StatsType.BLOCK_CACHE:
                self.block_cache_stats_mngr.add_lines(time, cf_name,
                                                      stats_lines_to_parse)
            elif stats_type == StatsMngr.StatsType.CF_NO_FILE:
                self.cf_no_file_stats_mngr.add_lines(time, cf_name,
                                                     stats_lines_to_parse)
            elif stats_type == StatsMngr.StatsType.CF_FILE_HISTOGRAM:
                self.cf_file_histogram_stats_mngr.add_lines(
                    time, cf_name, stats_lines_to_parse)
            else:
                valid_stats_type = False
        except utils.ParsingError as e:  # noqa
            logging.exception(format_err_msg(
                f"Error parsing a Stats Entry. time:{time}, cf:{cf_name}" +
                str(ErrContext(**{
                    "log_line_idx": entry_start_line_num + start_line_idx,
                    "log_line": db_stats_lines[start_line_idx]
                }))))

            valid_stats_type = True

        if not valid_stats_type:
            assert False, f"Unexpected stats type ({stats_type})"

    def try_adding_entries(self, log_entries, start_entry_idx):
        cf_names_found = set()
        entry_idx = start_entry_idx

        # Our entries starts with the "------- DUMPING STATS -------" entry
        if not StatsMngr.is_dump_stats_start(log_entries[entry_idx]):
            return False, entry_idx, cf_names_found

        logging.debug(f"Parsing Stats Dump Entry ("
                      f"{format_line_num_from_entry(log_entries[entry_idx])}")

        entry_idx += 1

        db_stats_entry = log_entries[entry_idx]
        db_stats_lines = \
            utils.remove_empty_lines_at_start(
                db_stats_entry.get_msg_lines())
        db_stats_time = db_stats_entry.get_time()
        # "** DB Stats **" must be next (allowing empty lines until it arrives)
        assert len(db_stats_lines) > 0
        assert DbWideStatsMngr.is_start_line(db_stats_lines[0])

        def log_parsing_error(msg_prefix):
            logging.error(format_err_msg(
                f"{msg_prefix} While parsing Stats Entry. time:"
                f"{db_stats_time}, cf:{curr_cf_name}",
                ErrContext(**{
                    "log_line_idx":
                        db_stats_entry.get_start_line_num() + line_idx})))

        line_idx = 0
        stats_type = StatsMngr.StatsType.DB_WIDE
        curr_cf_name = utils.NO_CF
        try:
            while line_idx < len(db_stats_lines):
                next_line_num, next_stats_type, next_cf_name = \
                    StatsMngr.find_next_start_line_in_db_stats(db_stats_lines,
                                                               line_idx,
                                                               stats_type)
                # parsing must progress
                assert next_line_num > line_idx

                self.parse_next_db_stats_entry_lines(
                    db_stats_time,
                    curr_cf_name,
                    stats_type,
                    db_stats_entry.get_start_line_num(),
                    db_stats_lines,
                    line_idx,
                    next_line_num)

                line_idx = next_line_num
                stats_type = next_stats_type

                if next_cf_name is not None:
                    curr_cf_name = next_cf_name
                    if next_cf_name != utils.NO_CF:
                        cf_names_found.add(curr_cf_name)
        except AssertionError:
            log_parsing_error("Assertion")
            raise
        except Exception:  # noqa
            log_parsing_error("Exception")
            raise

        # Done parsing the stats entry
        entry_idx += 1

        line_num = format_line_num_from_entry(log_entries[entry_idx]) \
            if entry_idx < len(log_entries) else \
            format_line_num_from_line_idx(log_entries[-1].get_end_line_idx())
        logging.debug(f"Completed Parsing Stats Dump Entry ({line_num})")

        return True, entry_idx, cf_names_found

    def get_db_wide_stats_mngr(self):
        return self.db_wide_stats_mngr

    def get_compactions_stats_mngr(self):
        return self.compaction_stats_mngr

    def get_blob_stats_mngr(self):
        return self.blob_stats_mngr

    def get_block_cache_stats_mngr(self):
        return self.block_cache_stats_mngr

    def get_cf_no_file_stats_mngr(self):
        return self.cf_no_file_stats_mngr

    def get_cf_file_histogram_stats_mngr(self):
        return self.cf_file_histogram_stats_mngr
