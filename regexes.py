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

#
# BASIC TYPES AND CONSTRUCTS
#
WS = r'\s*'
INT = r'[\d]+'
INT_C = r"([\d]+)"
FLOAT = r'[-+]?(?:\d+(?:[.,]\d*)?|[.,]\d+)(?:[eE][-+]?\d+)?'
FLOAT_C = fr'({FLOAT})'
NUM_UNIT = r'(K|M|G)'
BYTES_UNIT = r'(KB|MB|GB|TB)'
NUM_WITH_UNIT = fr'{FLOAT_C}\s*{NUM_UNIT}?\s*'
NUM_WITH_UNIT_ONLY = fr"{NUM_WITH_UNIT}\Z"
NUM_BYTES_WITH_UNIT = fr'{FLOAT_C}\s*{BYTES_UNIT}?\s*'
NUM_BYTES_WITH_UNIT_ONLY = fr'{NUM_BYTES_WITH_UNIT}\Z'
CF_NAME_OLD = r'\[(?P<cf>[\w\]]*)\]'
CF_NAME = r'\[(?P<cf>.*)\]'
CF_ID = fr'\(ID\s+(?P<cf_id>{INT})\)'
JOB_ID = r"\[JOB (?P<job_id>[\d+]+)\]"
POINTER_NC = r'0x[\dA-Fa-f]+'
POINTER = fr'({POINTER_NC})'
SANITIZED_POINTER = fr"Pointer \((?P<ptr>{POINTER_NC})\)"

#
# LOG ENTRY PARTS REGEXES
#
EMPTY_LINE = r'^\s*$'
TIMESTAMP = r'\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}'
ORIG_TIME = fr"\(Original Log Time ({TIMESTAMP})\)"
# Example: [/flush_job.cc:858]
CODE_POS = r"\[\/?.*?\.[\w:]+:\d+\]"

START_LINE_WITH_WARN_PARTS = \
    fr"({TIMESTAMP}) (\w+)\s*(?:{ORIG_TIME})?\s*" \
    fr"\[(WARN|ERROR|FATAL)\]\s*({CODE_POS})?(.*)"
START_LINE_PARTS = \
    fr"({TIMESTAMP}) (\w+)\s*" \
    fr"(?:{ORIG_TIME})?\s*({CODE_POS})?(.*)"

# A log entry that has the cf-name followed by the job id
# Example [column_family_name_000001] [JOB 31]
CF_WITH_JOB_ID = fr"{CF_NAME_OLD}\s*{JOB_ID}"

#
# METADATA REGEXES
DB_SESSION_ID = r"DB Session ID:\s*([0-9A-Z]+)"

# <product name> version: <version number>
PRODUCT_AND_VERSION = r"(\S+) version: ([0-9.]+)"

GIT_HASH_LINE = r"Git sha \s*(\S+)"

#
# OPTIONS REGEXES
OPTION_LINE = r"\s*Options\.(\S+)\s*:\s*(.+)?"
DB_WIDE_WBM_PSEUDO_OPTION_LINE = r"\s*wbm\.(\S+)\s*:\s*(.+)"

# example:
# --------------- Options for column family [default]:
# In case of a match the result will be [<column-family name>] (List)
CF_OPTIONS_START = \
    r"--------------- Options for column family \[(.*)\]:.*"

TABLE_OPTIONS_START_LINE = \
    r"^\s*table_factory options:\s*(\S+)\s*:(.*)"
TABLE_OPTIONS_CONTINUATION_LINE = r"^\s*(\S+)\s*:(.*)"

#
# EVENTS
#
PREAMBLE_EVENT = r"\[(.*?)\] \[JOB ([0-9]+)\]\s*(.*)"

# [column_family_name_000018] [JOB 38] Flushing memtable with next log file: 5
# Capturing: cf_name, job_id, wal-id
FLUSH_EVENT_PREAMBLE = \
    fr"^{WS}{CF_NAME}{WS}{JOB_ID}{WS}Flushing memtable " \
    fr"with next log file:{WS}(?P<wal_id>{INT})"

# [default] [JOB 13] Compacting 1@1 + 5@2 files to L2, score 1.63
# Capturing: cf_name, job_id
COMPACTION_EVENT_PREAMBLE = \
    fr"^{WS}{CF_NAME}{WS}{JOB_ID}{WS}Compacting.*score"

EVENT = r"\s*EVENT_LOG_v1"


WRITE_DELAY_WARN_MSG = fr"{CF_NAME_OLD}{WS}Stalling writes"
WRITE_STOP_WARN_MSG = fr"{CF_NAME_OLD}{WS}Stopping writes"

#
# STATISTICS RELATED
#
DUMP_STATS_STR = r'------- DUMPING STATS -------'
DB_STATS = fr'^{WS}\*\* DB Stats \*\*{WS}$'
COMPACTION_STATS = fr'^{WS}\*\* Compaction Stats{WS}{CF_NAME}{WS}\*\*{WS}$'

FILE_READ_LATENCY_STATS = \
    fr'^{WS}\*\* File Read Latency Histogram By Level{WS}{CF_NAME}' \
    fr'{WS}\*\*{WS}$'

LEVEL_READ_LATENCY_LEVEL_LINE = \
    fr'\*\* Level {INT_C} read latency histogram \(micros\):'

# Count: 26687513 Average: 3.1169  StdDev: 34.39
LEVEL_READ_LATENCY_STATS_LINE1 = \
    fr"Count:{WS}{INT_C}{WS}Average:{WS}{FLOAT_C}{WS}StdDev:{WS}{FLOAT_C}"

# Min: 0  Median: 2.4427  Max: 56365
LEVEL_READ_LATENCY_STATS_LINE2 = \
    fr"Min:{WS}{INT_C}{WS}Median:{WS}{FLOAT_C}{WS}Max:{WS}{INT_C}"

STATS_COUNTERS_AND_HISTOGRAMS = r'^\s*STATISTICS:\s*$'

# Uptime(secs): 603.0 total, 600.0 interval
UPTIME_STATS_LINE = \
    fr'^{WS}Uptime\(secs\):{WS}(?P<total>{FLOAT}){WS}total,' \
    fr'{WS}(?P<interval>{FLOAT}){WS}interval'

#
# BLOCK CACHE STATS REGEXES
#
CACHE_ID = r"(\S+)"

# LRUCache@0x556ae9ce9770#27411
# <name>@<ptr>#<process-id>
CACHE_ID_PARTS = fr"(?P<name>.*?)@(?P<ptr>{POINTER_NC})#(?P<pid>{INT})"

BLOCK_CACHE_STATS_START = \
    fr'Block cache {CACHE_ID} capacity: {FLOAT_C} {BYTES_UNIT} '
BLOCK_CACHE_ENTRY_STATS = \
    r"Block cache entry stats\(count,size,portion\): (.*)"
BLOCK_CACHE_CF_ENTRY_STATS = fr"Block cache {CF_NAME} (.*)"
BLOCK_CACHE_ENTRY_ROLES_NAMES = r"([A-Za-z]+)\("
BLOCK_CACHE_ENTRY_ROLES_STATS = r"[a-zA-Z]+\(([^\)]+?)\)"
STATS_COUNTER = fr"^{WS}([\w\.]+){WS}COUNT{WS}:{WS} {INT_C}{WS}$"

STATS_HISTOGRAM = \
    fr'^\s*([\w\.]+) P50 : {FLOAT_C} P95 : {FLOAT_C} P99 : {FLOAT_C} ' \
    fr'P100 : {FLOAT_C} COUNT : {INT_C} SUM : {INT_C}'
STATS_HISTOGRAM = \
    fr'^{WS}(?P<name>[\w\.]+){WS}P50{WS}:{WS}(?P<P50>{FLOAT})' \
    fr'{WS}P95{WS}:{WS}(?P<P95>{FLOAT}){WS}P99{WS}:{WS}(?P<P99>{FLOAT})' \
    fr'{WS}P100{WS}:{WS}(?P<P100>{FLOAT})' \
    fr'{WS}COUNT{WS}:{WS}(?P<count>{INT}){WS}SUM{WS}:{WS}(?P<sum>{INT})'

BLOB_STATS_LINE = \
    fr'Blob file count: ([\d]+), total size: {FLOAT_C} GB, ' \
    fr'garbage size: {FLOAT_C} GB, space amp: {FLOAT_C}'

SUPPORT_INFO_START_LINE = r'\s*Compression algorithms supported:\s*$'

VERSION = r'(\d+)\.(\d+)\.?(\d+)?'

# Interval stall: 00:00:0.000 H:M:S, 0.0 percent
DB_WIDE_INTERVAL_STALL = \
    fr"Interval stall: (\d+):(\d+):(\d+)\.(\d+) H:M:S, {FLOAT_C} percent"

#  Cumulative stall: 00:00:0.000 H:M:S, 0.0 percent
DB_WIDE_CUMULATIVE_STALL = \
    fr"Cumulative stall: (\d+):(\d+):(\d+)\.(\d+) H:M:S, {FLOAT_C} percent"

# Cumulative writes: 819K writes, 1821M keys, 788K commit groups, 1.0 writes per commit group, ingest: 80.67 GB, 68.66 MB/s # noqa
DB_WIDE_CUMULATIVE_WRITES = \
    fr"Cumulative writes:\s*{NUM_WITH_UNIT} writes,\s*{NUM_WITH_UNIT} keys.*"\
    fr"ingest: {FLOAT_C}\s*GB,\s*{FLOAT_C}\s*MB/s"

CF_STALLS_LINE_START = "Stalls(count):"
CF_STALLS_COUNT_AND_REASON = r"\b(\d+) (.*?),"
CF_STALLS_INTERVAL_COUNT = r".*interval (\d+) total count$"

# 2022/12/17-11:12:17.399948 38990 [/version_set.cc:4980] Column family [column_family_name_000009] (ID 9), log number is 41576 # noqa
RECOVERED_CF = \
    fr"Column family {CF_NAME}\s*{CF_ID},{WS}log number is (?P<log_num>{INT})"

# 2022/12/17-06:02:37.695926 24554 [/db_impl/db_impl.cc:2799] Created column family [column_family_name_000001] (ID 1) # noqa
CREATE_CF = fr"Created column family {CF_NAME}\s*{CF_ID}"

DROP_CF = fr"Dropped column family with id {INT_C}\s*"


ROCKSDB_BASELINE_LOG_FILE = r"LOG-rocksdb-(\d+\.\d+\.?\d*)"
SPEEDB_BASELINE_LOG_FILE = r"LOG-speedb-(\d+\.\d+\.?\d*)"

# 2021/05/04-20:54:25.385487 7fa4245ff700 [/compaction/compaction_job.cc:1762] [default] [JOB 38680] Compacting 1@5 + 1@6 files to L6, score 0.9 # noqa
COMPACTION_BEFORE_SCORE_LINE = \
    fr"{CF_NAME}\s*{JOB_ID}\s*Compacting .*" \
    fr"files to L{INT_C},\s*score\s*{FLOAT_C}"

# A line in a compaction job preceding the compaction_finished event
# [default] compacted to: files[4 7 45 427 822 0 0] max score 1.63,
# MB/sec: 450.9 rd, 450.1 wr, level 1, files in(4, 3) out(7 +0 blob)
# MB in(224.7, 193.2 +0.0 blob) out(417.0 +0.0 blob), read-write-amplify(3.7)
# write-amplify(1.9) OK, records in: 424286, records dropped: 789
# output_compression: NoCompression
# The following regex matches this line and extracts the following fields:
# 0: cf name
# 1: MB/sec - <Rate> rd
# 2: MB/sec - <Rate> wr
# 3: read-write-amplify
# 4: write-amplify
# 5: records in
# 6: records dropped
#
COMPACTION_JOB_FINISH_STATS_LINE = \
    fr"{CF_NAME_OLD}.*,\s*MB\/sec:\s*{FLOAT_C}\s*rd," \
    fr"\s*{FLOAT_C}\s*wr,.*read-write-amplify\({FLOAT_C}\)\s*write-amplify\(" \
    fr"{FLOAT_C}\).*records in:\s*{INT_C},\s*records dropped:\s*{INT_C}"
