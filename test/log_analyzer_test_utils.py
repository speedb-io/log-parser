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

import log_entry


def read_sample_file(file_name, expected_num_entries):
    file_path = "input_files/" + file_name
    f = open(file_path)
    lines = f.readlines()
    entries = []
    entry = None
    for i, line in enumerate(lines):
        if log_entry.LogEntry.is_entry_start(line):
            if entry:
                entries.append(entry.all_lines_added())
            entry = log_entry.LogEntry(i, line)
        else:
            assert entry
            entry.add_line(line)

    if entry:
        entries.append(entry.all_lines_added())

    assert len(entries) == expected_num_entries

    return entries
