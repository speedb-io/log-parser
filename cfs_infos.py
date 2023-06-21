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
from enum import Enum, auto

import regexes
import utils

get_error_context = utils.get_error_context_from_entry


class CfDiscoveryType(Enum):
    OPTIONS = 1
    CREATE_NO_OPTIONS = 2
    RECOVERED_NO_OPTIONS = 3
    DURING_PARSING = 4


@dataclass
class CfMetadata:
    discovery_type: CfDiscoveryType
    name: str
    discovery_time: str
    has_options: bool
    auto_generated: bool
    id: int = None
    drop_time: str = None

    def __lt__(self, other):
        assert isinstance(other, CfMetadata)

        if self.id is not None and other.id is not None:
            return self.id < other.id
        if self.discovery_type.value != other.discovery_type.value:
            return self.discovery_type.value < other.discovery_type.value
        if self.discovery_type == CfDiscoveryType.OPTIONS or \
                self.discovery_type == CfDiscoveryType.CREATE_NO_OPTIONS or \
                self.discovery_type == CfDiscoveryType.RECOVERED_NO_OPTIONS:
            return self.discovery_time < other.discovery_time
        return self.name < other.name


class CfOper(Enum):
    CREATE = auto()
    RECOVER = auto()
    DROP = auto()


class CfsMetadata:
    def __init__(self, log_file_path):
        self.cfs_info = {}
        self.log_file_path = log_file_path

    def add_cf_found_during_cf_options_parsing(self, cf_name, cf_id,
                                               is_auto_generated, entry):
        if not self._validate_cf_doesnt_exist(cf_name, entry):
            return False

        self.cfs_info[cf_name] = \
            CfMetadata(discovery_type=CfDiscoveryType.OPTIONS,
                       name=cf_name,
                       discovery_time=entry.get_time(),
                       has_options=True,
                       auto_generated=is_auto_generated,
                       id=cf_id)
        return True

    def handle_cf_name_found_during_parsing(self, cf_name, entry, cf_id=None):
        if cf_name in self.cfs_info:
            return False

        logging.info(f"Found new cf name during parsing [{cf_name}]")
        self.cfs_info[cf_name] = \
            CfMetadata(discovery_type=CfDiscoveryType.DURING_PARSING,
                       name=cf_name,
                       discovery_time=entry.get_time(),
                       has_options=False,
                       auto_generated=False,
                       id=cf_id)
        return True

    def try_parse_as_cf_lifetime_entries(self, log_entries, entry_idx):
        # CF lifetime messages in log are currently always single liners
        entry = log_entries[entry_idx]
        next_entry_idx = entry_idx + 1

        try:
            # The cf lifetime entries are printed when the CF will have been
            # discovered
            if self.try_parse_as_drop_cf(entry):
                return True, next_entry_idx
            if self.try_parse_as_recover_cf(entry):
                return True, next_entry_idx
            if self.try_parse_as_create_cf(entry):
                return True, next_entry_idx

            return None, entry_idx

        except utils.ParsingError as e:
            logging.error(
                f"Failed parsing entry as cf lifetime entry ({e}).\n "
                f"entry:{entry}")
            return True, next_entry_idx

    def try_parse_as_drop_cf(self, entry):
        cf_id_match = re.findall(regexes.DROP_CF, entry.get_msg())
        if not cf_id_match:
            return False

        assert len(cf_id_match) == 1

        dropped_cf_id = cf_id_match[0]
        # A dropped cf may have been discovered without knowing its id (
        # e.g., auto-generated one)
        cf_info = self.get_cf_info_by_id(dropped_cf_id)
        if not cf_info:
            logging.info(utils.format_err_msg(
                f"CF with Id {dropped_cf_id} "
                f"dropped but no cf with matching id.", error_context=None,
                entry=entry, file_path=self.log_file_path))
            return True

        self._validate_cf_wasnt_dropped(cf_info, entry, raise_exception=True)
        cf_info.drop_time = entry.get_time()

        return True

    def try_parse_as_recover_cf(self, entry):
        cf_match = re.search(regexes.RECOVERED_CF, entry.get_msg())
        if not cf_match:
            return False
        assert len(cf_match.groups()) == 3

        cf_name = cf_match.group('cf')
        cf_id = int(cf_match.group('cf_id'))

        if self._does_cf_exist(cf_name):
            self._update_cf_id(cf_name, cf_id, entry, raise_exception=True)
        else:
            logging.info(f"Created cf without options [{cf_name}]")
            self.cfs_info[cf_name] = \
                CfMetadata(discovery_type=CfDiscoveryType.RECOVERED_NO_OPTIONS,
                           name=cf_name,
                           discovery_time=entry.get_time(),
                           has_options=False,
                           auto_generated=False,
                           id=cf_id)
        return True

    def try_parse_as_create_cf(self, entry):
        cf_match = re.search(regexes.CREATE_CF, entry.get_msg())
        if not cf_match:
            return False

        assert len(cf_match.groups()) == 2

        cf_name = cf_match.group('cf')
        cf_id = int(cf_match.group('cf_id'))

        if self._does_cf_exist(cf_name):
            self._update_cf_id(cf_name, cf_id, entry, raise_exception=True)
        else:
            logging.info(f"Created cf without options [{cf_name}]")
            self.cfs_info[cf_name] = \
                CfMetadata(discovery_type=CfDiscoveryType.CREATE_NO_OPTIONS,
                           name=cf_name,
                           discovery_time=entry.get_time(),
                           has_options=False,
                           auto_generated=False,
                           id=cf_id)

        return True

    def parsing_complete(self):
        self.cfs_info = utils.sort_dict_on_values(self.cfs_info)

    def set_cf_id(self, cf_name, cf_id, line, line_idx):
        self._validate_cf_exists(cf_name, line, line_idx)

        cf_id = int(cf_id)
        self._update_cf_id(cf_name, cf_id, line, line_idx)

        self.cfs_info[cf_name].id = int(cf_id)

    def get_cf_id(self, cf_name):
        cf_id = None
        if cf_name in self.cfs_info:
            cf_id = self.cfs_info[cf_name].id
        return cf_id

    def get_cf_info_by_name(self, cf_name):
        cf_info = None
        if cf_name in self.cfs_info:
            cf_info = self.cfs_info[cf_name]
        return cf_info

    def get_cf_info_by_id(self, cf_id):
        cf_id = int(cf_id)
        for cf_info in self.cfs_info.values():
            if cf_info.id == cf_id:
                return cf_info

        return None

    def get_non_auto_generated_cfs_names(self):
        return [cf_name for cf_name in self.cfs_info.keys() if
                not self.cfs_info[cf_name].auto_generated]

    def get_auto_generated_cf_names(self):
        return [cf_name for cf_name in self.cfs_info.keys() if
                self.cfs_info[cf_name].auto_generated]

    def get_cfs_names_that_have_options(self, include_auto_generated):
        returned_cfs_names = []
        for cf_name in self.cfs_info.keys():
            if self.cfs_info[cf_name].has_options:
                if include_auto_generated or \
                        not self.cfs_info[cf_name].auto_generated:
                    returned_cfs_names.append(cf_name)
        return returned_cfs_names

    def get_all_cfs_names(self):
        return list(self.cfs_info.keys())

    def get_num_cfs(self):
        return len(self.cfs_info) if self.cfs_info else 0

    def are_there_auto_generated_cf_names(self):
        return len(self.get_auto_generated_cf_names()) > 0

    def get_num_cfs_when_certain(self):
        # Once a log has auto-generated cf names we have no certain way of
        # knowing how many cf-s there are
        if self.are_there_auto_generated_cf_names():
            return None
        else:
            return self.get_num_cfs()

    def was_cf_dropped(self, cf_name):
        cf_info = self.get_cf_info_by_name(cf_name)
        if not cf_info:
            return False
        return cf_info.drop_time is not None

    def get_cf_drop_time(self, cf_name):
        drop_time = None
        cf_info = self.get_cf_info_by_name(cf_name)
        if cf_info:
            drop_time = cf_info.drop_time
        return drop_time

    def _validate_cf_exists(self, cf_name, entry):
        if cf_name not in self.cfs_info:
            raise utils.ParsingError(
                f"cf ({cf_name}) doesn't exist.",
                self.get_error_context(entry))

    def _does_cf_exist(self, cf_name):
        return cf_name in self.cfs_info

    def _validate_cf_doesnt_exist(self, cf_name, entry):
        if self._does_cf_exist(cf_name):
            context = self.get_error_context(entry)
            logging.error(f"cf ({cf_name}) already exists.{context}")
            return False
        return True

    def _update_cf_id(self, cf_name, cf_id, entry, raise_exception):
        curr_cf_id = self.cfs_info[cf_name].id
        if curr_cf_id and curr_cf_id != int(cf_id):
            context = self.get_error_context(entry)
            if raise_exception:
                raise utils.ParsingError(
                    f"id ({cf_id}) of cf ({cf_name}) already exists.", context)
            else:
                logging.error(
                    f"id ({cf_id}) of cf ({cf_name}) already exists.{context}")
                return False

        self.cfs_info[cf_name].id = cf_id
        return True

    def _validate_cf_wasnt_dropped(self, cf_info, entry, raise_exception):
        if cf_info.drop_time:
            context = self.get_error_context(entry)
            if raise_exception:
                raise utils.ParsingError(
                    f"CF ({cf_info.name}) already dropped at "
                    f"({cf_info.drop_time}).", context)
            else:
                logging.error(f"CF ({cf_info.name}) already dropped at "
                              f"({cf_info.drop_time}).{context}")
                return False
        return True

    def get_error_context(self, entry):
        if entry is None:
            return None
        return utils.get_error_context_from_entry(entry, self.log_file_path)
