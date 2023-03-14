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

import copy
import re
from enum import Enum, auto

import regexes
import utils

DB_WIDE_CF_NAME = utils.NO_CF

SANITIZED_NO_VALUE = "Missing"
RAW_NULL_PTR = "Uninitialised"
SANITIZED_NULL_PTR = f"Pointer ({RAW_NULL_PTR})"
SANITIZED_FALSE = str(False)
SANITIZED_TRUE = str(False)


class SectionType(str, Enum):
    VERSION = "Version"
    DB_WIDE = "DBOptions"
    CF = "CFOptions"
    TABLE_OPTIONS = "TableOptions.BlockBasedTable"

    def __str__(self):
        return str(self.value)

    @staticmethod
    def extract_section_type(full_option_name):
        # Try to match a section type by combining dot-delimited words
        # in the full name. Longer combinations are tested before shorter
        # ones ("TableOptions.BlockBasedTable" will be tested before
        # "CFOptions")
        name_parts = full_option_name.split('.')
        for i in range(len(name_parts)):
            try:
                potential_section_type = '.'.join(name_parts[:-i])
                return SectionType(potential_section_type)
            except Exception: # noqa
                continue

        raise utils.ParsingError(
            f"Invalid full option name ({full_option_name}")


def validate_section(section_type, expected_type=None):
    SectionType.extract_section_type(f"{section_type}.dummy")
    if expected_type is not None:
        if section_type != expected_type:
            raise utils.ParsingError(
                f"Not DB Wide section name ({section_type}")


def parse_full_option_name(full_option_name):
    section_type = SectionType.extract_section_type(full_option_name)
    option_name = full_option_name.split('.')[-1]
    return section_type, option_name


def extract_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    return option_name


def get_full_option_name(section_type, option_name):
    validate_section(section_type)
    return f"{section_type}.{option_name}"


def get_db_wide_full_option_name(option_name):
    return get_full_option_name(SectionType.DB_WIDE, option_name)


def extract_db_wide_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    validate_section(section_type, SectionType.DB_WIDE)
    return option_name


def get_cf_full_option_name(option_name):
    return get_full_option_name(SectionType.CF, option_name)


def extract_cf_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    validate_section(section_type, SectionType.CF)
    return option_name


def get_cf_table_full_option_name(option_name):
    return get_full_option_name(SectionType.TABLE_OPTIONS, option_name)


def extract_cf_table_option_name(full_option_name):
    section_type, option_name = parse_full_option_name(full_option_name)
    validate_section(section_type, SectionType.TABLE_OPTIONS)
    return option_name


def is_sanitized_pointer_value(value):
    return re.findall(regexes.SANITIZED_POINTER, value.strip())


def sanitized_to_raw_ptr_value(sanitized_value):
    if sanitized_value.strip() == SANITIZED_NULL_PTR:
        return RAW_NULL_PTR
    assert is_sanitized_pointer_value(sanitized_value)

    match = re.fullmatch(regexes.SANITIZED_POINTER, sanitized_value.strip())
    assert match
    return match.group('ptr')


class SanitizedValueType(Enum):
    NO_VALUE = auto()
    BOOL = auto()
    NULL_PTR = auto()
    POINTER = auto()
    OTHER = auto()

    @staticmethod
    def get_type_from_str(value):
        if value == SANITIZED_NO_VALUE:
            return SanitizedValueType.NO_VALUE
        elif value == SANITIZED_NULL_PTR:
            return SanitizedValueType.NULL_PTR
        elif value == SANITIZED_FALSE or value == SANITIZED_TRUE:
            return SanitizedValueType.BOOL
        elif is_sanitized_pointer_value(value):
            return SanitizedValueType.POINTER
        else:
            return SanitizedValueType.OTHER


def check_and_sanitize_if_null_ptr(value):
    if isinstance(value, str):
        temp_value = value.lower()
        if temp_value == "none" or \
                temp_value == "(nil)" or \
                temp_value == "nil" or \
                temp_value == "nullptr" or \
                temp_value == "null" or \
                temp_value == "0x0":
            return True, SANITIZED_NULL_PTR

    return False, value


def check_and_sanitize_if_bool_value(value, include_int):
    if isinstance(value, bool):
        return True, str(value)
    elif isinstance(value, str):
        temp_value = value.lower()
        if temp_value == "false":
            return True, str(False)
        elif temp_value == "true":
            return True, str(True)
        elif include_int and temp_value == '0':
            return True, str(False)
        elif include_int and temp_value == '1':
            return True, str(True)
        else:
            return False, value
    elif include_int and isinstance(value, int):
        is_bool = value == 0 or value == 1
        if is_bool:
            return True, str(bool(value))

    return False, value


def check_and_sanitize_if_pointer_value(value):
    is_null, sanitized_value = check_and_sanitize_if_null_ptr(value)
    if is_null:
        return False, value
    if not isinstance(value, str):
        return False, value

    pointer_match = re.findall(regexes.POINTER, value.strip())
    if not pointer_match:
        return False, value
    assert len(pointer_match) == 1

    return True, f"Pointer ({pointer_match[0]})"


def get_sanitized_value_with_type(value):
    if value is None:
        return SANITIZED_NO_VALUE, SanitizedValueType.NO_VALUE

    is_bool_value, sanitized_value =\
        check_and_sanitize_if_bool_value(value, include_int=False)
    if is_bool_value:
        return sanitized_value, SanitizedValueType.BOOL

    is_null_ptr, sanitized_value = check_and_sanitize_if_null_ptr(value)
    if is_null_ptr:
        return sanitized_value, SanitizedValueType.NULL_PTR

    is_pointer_value, sanitized_value =\
        check_and_sanitize_if_pointer_value(value)
    if is_pointer_value:
        return sanitized_value, SanitizedValueType.POINTER

    return value, SanitizedValueType.OTHER


def get_sanitized_value(value):
    sanitized_value, _ = get_sanitized_value_with_type(value)
    return sanitized_value


def get_sanitized_options_diff(base_value, new_value, expect_diff):
    sanitized_base_value, base_type = \
        get_sanitized_value_with_type(base_value)
    sanitized_new_value, new_type = \
        get_sanitized_value_with_type(new_value)

    # We expect a diff => It's a bug if both are no-value (=> equal)
    if expect_diff:
        assert base_type != SanitizedValueType.NO_VALUE or \
           new_type != SanitizedValueType.NO_VALUE
        # Same - 2 pointers are considered equal => bug
        assert base_type != SanitizedValueType.POINTER or \
               new_type != SanitizedValueType.POINTER

    if base_type == SanitizedValueType.BOOL or \
            new_type == SanitizedValueType.BOOL:
        _, sanitized_base_value = check_and_sanitize_if_bool_value(
            sanitized_base_value, include_int=True)
        _, sanitized_new_value = check_and_sanitize_if_bool_value(
            sanitized_new_value, include_int=True)

    if base_type == new_type and base_type == SanitizedValueType.POINTER:
        are_diff = False
    else:
        are_diff = sanitized_base_value != sanitized_new_value

    if expect_diff:
        assert are_diff
        return sanitized_base_value, sanitized_new_value
    else:
        return are_diff, sanitized_base_value, sanitized_new_value


def are_non_sanitized_values_different(base_value, new_value):
    are_diff, _, _ = get_sanitized_options_diff(base_value, new_value,
                                                expect_diff=False)

    return are_diff


class FullNamesOptionsDict:
    # {Full-option-name: {<cf>: <option-value>}}
    def __init__(self, options_dict=None):
        self.options_dict = {}
        if options_dict is not None:
            self.options_dict = copy.deepcopy(options_dict)

    def __eq__(self, other):
        if isinstance(other, FullNamesOptionsDict):
            return self.options_dict == other.options_dict
        elif isinstance(other, dict):
            return self.options_dict == other
        else:
            assert False, "Comparing to an invalid type " \
                          f"({type(other)})"

    def init_from_full_names_options_no_cf_dict(
            self, cf_name, options_dict_no_cf):
        # Input: {Full-option-name: <option-value>}
        # Converts to the format of this class using the specified cf_name
        assert self.options_dict == {}

        for full_option_name, option_value in options_dict_no_cf.items():
            self.options_dict[full_option_name] = {cf_name: option_value}

    def set_option(self, section_type, cf_name, option_name, option_value):
        if section_type == SectionType.DB_WIDE:
            assert cf_name == DB_WIDE_CF_NAME

        full_option_name = get_full_option_name(section_type, option_name)
        if full_option_name not in self.options_dict:
            self.options_dict[full_option_name] = {}

        self.options_dict[full_option_name].update({
            cf_name: get_sanitized_value(option_value)})

    def get_option_by_full_name(self, full_option_name):
        if full_option_name not in self.options_dict:
            return None
        return self.options_dict[full_option_name]

    def get_option_by_parts(self, section_type, option_name, cf_name=None):
        value = self.get_option_by_full_name(get_full_option_name(
            section_type, option_name))
        if value is None or cf_name is None:
            return value
        if cf_name not in value:
            return None
        return value[cf_name]

    def set_db_wide_option(self, option_name, option_value):
        self.set_option(SectionType.DB_WIDE, DB_WIDE_CF_NAME, option_name,
                        option_value)

    def get_db_wide_option(self, option_name):
        return self.get_option_by_parts(SectionType.DB_WIDE, option_name,
                                        DB_WIDE_CF_NAME)

    def set_cf_option(self, cf_name, option_name, option_value):
        self.set_option(SectionType.CF, cf_name, option_name,
                        option_value)

    def get_cf_option(self, option_name, cf_name=None):
        return self.get_option_by_parts(SectionType.CF, option_name, cf_name)

    def set_cf_table_option(self, cf_name, option_name, option_value):
        self.set_option(SectionType.TABLE_OPTIONS, cf_name, option_name,
                        option_value)

    def get_cf_table_option(self, option_name, cf_name=None):
        return self.get_option_by_parts(SectionType.TABLE_OPTIONS,
                                        option_name, cf_name)

    def get_options_dict(self):
        return self.options_dict


class OptionsDiff:
    def __init__(self, baseline_options, new_options):
        self.baseline_options = baseline_options
        self.new_options = new_options
        self.diff_dict = {}

    def __eq__(self, other):
        if isinstance(other, OptionsDiff):
            return self.diff_dict == other.diff_dict
        elif isinstance(other, dict):
            return self.diff_dict == other
        else:
            assert False, "Comparing to an invalid type " \
                          f"({type(other)})"

    def diff_in_base(self, cf_name, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name][cf_name] =\
            get_sanitized_options_diff(
                self.baseline_options[full_option_name][cf_name], None,
                expect_diff=True)

    def diff_in_new(self, cf_name, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name][cf_name] =\
            get_sanitized_options_diff(
                None, self.new_options[full_option_name][cf_name],
                expect_diff=True)

    def diff_between(self, cf_name, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name][cf_name] =\
            get_sanitized_options_diff(
                self.baseline_options[full_option_name][cf_name],
                self.new_options[full_option_name][cf_name],
                expect_diff=True)

    def add_option_if_necessary(self, full_option_name):
        if full_option_name not in self.diff_dict:
            self.diff_dict[full_option_name] = {}

    def is_empty_diff(self):
        return self.diff_dict == {}

    def get_diff_dict(self):
        return self.diff_dict


class CfsOptionsDiff:
    CF_NAMES_KEY = "cf names"

    def __init__(self, baseline_options, baseline_cf_name,
                 new_options, new_cf_name, diff_dict=None):
        self.baseline_options = baseline_options
        self.baseline_cf_name = baseline_cf_name
        self.new_options = new_options
        self.new_cf_name = new_cf_name
        self.diff_dict = {}
        if diff_dict is not None:
            self.diff_dict = diff_dict

    def __eq__(self, other):
        new_dict = None
        if isinstance(other, CfsOptionsDiff):
            new_dict = other.get_diff_dict()
        elif isinstance(other, dict):
            new_dict = other
        else:
            assert False, "Comparing to an invalid type " \
                          f"({type(other)})"
        assert CfsOptionsDiff.CF_NAMES_KEY in new_dict, \
            f"{CfsOptionsDiff.CF_NAMES_KEY} key missing in other"

        baseline_dict = self.get_diff_dict()
        assert baseline_dict[CfsOptionsDiff.CF_NAMES_KEY] == \
               new_dict[CfsOptionsDiff.CF_NAMES_KEY], (
            "Comparing diff entities for mismatching cf-s: "
            f"baseline:{baseline_dict[CfsOptionsDiff.CF_NAMES_KEY]}, "
            f"new:{new_dict[CfsOptionsDiff.CF_NAMES_KEY]}")

        return self.diff_dict == new_dict

    def diff_in_base(self, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name] = \
            get_sanitized_options_diff(
                self.baseline_options[full_option_name][self.baseline_cf_name],
                None, expect_diff=True)

    def diff_in_new(self, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name] = \
            get_sanitized_options_diff(
                None,
                self.new_options[full_option_name][self.new_cf_name],
                expect_diff=True)

    def diff_between(self, full_option_name):
        self.add_option_if_necessary(full_option_name)
        self.diff_dict[full_option_name] = \
            get_sanitized_options_diff(
                self.baseline_options[full_option_name][self.baseline_cf_name],
                self.new_options[full_option_name][self.new_cf_name],
                expect_diff=True)

    def add_option_if_necessary(self, full_option_name):
        if full_option_name not in self.diff_dict:
            self.diff_dict[full_option_name] = {}

    def is_empty_diff(self):
        return self.diff_dict == {}

    def get_diff_dict(self, exclude_compared_cfs_names=False):
        if self.is_empty_diff():
            return {}

        result_dict = self.diff_dict
        if not exclude_compared_cfs_names:
            result_dict.update({CfsOptionsDiff.CF_NAMES_KEY:
                                {"Base": self.baseline_cf_name,
                                 "New": self.new_cf_name}})
        return result_dict


class DatabaseOptions:
    def __init__(self, section_based_options_dict=None):
        # The options are stored in the following data structure:
        # {DBOptions: {DB_WIDE: {<option-name>: <option-value>, ...}},
        # {CFOptions: {<cf-name>: {<option-name>: <option-value>, ...}},
        # {TableOptions.BlockBasedTable:
        #   {<cf-name>: {<option-name>: <option-value>, ...}}}
        # If section_based_options_dict is specified, it must be in the
        # internal format (e.g., used when loading options from an options
        # file)
        self.options_dict = {}
        if section_based_options_dict:
            # TODO - Verify the format of the options_dict
            self.options_dict = section_based_options_dict

        self.cfs_names = None
        self.setup_column_families()

    def __str__(self):
        return "DatabaseOptions"

    def set_db_wide_options(self, options_dict):
        # The input dictionary is expected to be like this:
        # {<option name>: <option_value>}
        if self.are_db_wide_options_set():
            raise utils.ParsingAssertion(
                "DB Wide Options Already Set")

        self.options_dict[SectionType.DB_WIDE] = {
            DB_WIDE_CF_NAME: options_dict}
        self.setup_column_families()

    def create_section_if_necessary(self, section_type):
        if section_type not in self.options_dict:
            self.options_dict[section_type] = dict()

    def create_cf_in_section_if_necessary(self, section_type, cf_name):
        if cf_name not in self.options_dict[section_type]:
            self.cfs_names.append(cf_name)
            self.options_dict[section_type][cf_name] = dict()

    def create_section_and_cf_in_section_if_necessary(self, section_type,
                                                      cf_name):
        self.create_section_if_necessary(section_type)
        self.create_cf_in_section_if_necessary(section_type, cf_name)

    def validate_no_section(self, section_type):
        if section_type in self.options_dict:
            raise utils.ParsingAssertion(
                f"{section_type} Already Set")

    def validate_no_cf_name_in_section(self, section_type, cf_name):
        if cf_name in self.options_dict[section_type]:
            raise utils.ParsingAssertion(
                f"{section_type} Already Set for this CF ({cf_name})")

    def set_cf_options(self, cf_name, cf_non_table_options, table_options):
        # Both input dictionaries are expected to be like this:
        # {<option name>: <option_value>}
        self.create_section_if_necessary(SectionType.CF)
        self.validate_no_cf_name_in_section(SectionType.CF, cf_name)

        self.create_section_if_necessary(SectionType.TABLE_OPTIONS)
        self.validate_no_cf_name_in_section(SectionType.TABLE_OPTIONS,
                                            cf_name)

        self.options_dict[SectionType.CF][cf_name] = cf_non_table_options
        self.options_dict[SectionType.TABLE_OPTIONS][cf_name] = table_options

        self.setup_column_families()

    def setup_column_families(self):
        self.cfs_names = []

        if SectionType.CF in self.options_dict:
            self.cfs_names =\
                list(self.options_dict[SectionType.CF].keys())

        if SectionType.DB_WIDE in self.options_dict:
            self.cfs_names.extend(
                list(self.options_dict[SectionType.DB_WIDE].keys()))

    def are_db_wide_options_set(self):
        return SectionType.DB_WIDE in self.options_dict

    def get_cfs_names(self):
        cf_names_no_db_wide = copy.deepcopy(self.cfs_names)
        if DB_WIDE_CF_NAME in cf_names_no_db_wide:
            cf_names_no_db_wide.remove(DB_WIDE_CF_NAME)
        return cf_names_no_db_wide

    def set_db_wide_option(self, option_name, option_value,
                           allow_new_option=False):
        if not allow_new_option:
            assert self.get_db_wide_option(option_name) is not None,\
                "Trying to update a non-existent DB Wide Option." \
                f"{option_name} = {option_value}"

        self.create_section_and_cf_in_section_if_necessary(
            SectionType.DB_WIDE, DB_WIDE_CF_NAME)
        self.options_dict[SectionType.DB_WIDE][
            DB_WIDE_CF_NAME][option_name] = option_value

    def get_all_options(self):
        # Returns all the options that as a FullNamesOptionsDict
        full_options_names = []
        for section_type in self.options_dict:
            for cf_name in self.options_dict[section_type]:
                for option_name in self.options_dict[section_type][cf_name]:
                    full_option_name = get_full_option_name(section_type,
                                                            option_name)
                    full_options_names.append(full_option_name)

        return self.get_options(full_options_names)

    def get_db_wide_options(self):
        # Returns the DB-Wide options as a FullNamesOptionsDict
        full_options_names = []
        section_type = SectionType.DB_WIDE
        if section_type in self.options_dict:
            sec_dict = self.options_dict[section_type]
            for option_name in sec_dict[DB_WIDE_CF_NAME]:
                full_option_name = get_full_option_name(section_type,
                                                        option_name)
                full_options_names.append(full_option_name)

        return self.get_options(full_options_names, DB_WIDE_CF_NAME)

    def get_db_wide_options_for_display(self):
        options_for_display = {}
        db_wide_options = self.get_db_wide_options().get_options_dict()
        for full_option_name, option_value_with_cf in db_wide_options.items():
            option_name = extract_db_wide_option_name(full_option_name)
            option_value = option_value_with_cf[DB_WIDE_CF_NAME]
            options_for_display[option_name] = option_value
        return options_for_display

    def get_db_wide_option(self, option_name):
        # Returns the raw value of a single DB-Wide option
        full_option_name = get_full_option_name(SectionType.DB_WIDE,
                                                option_name)
        option_value_dict = \
            self.get_options([full_option_name]).get_options_dict()
        if not option_value_dict:
            return None
        else:
            return option_value_dict[full_option_name][DB_WIDE_CF_NAME]

    def get_cf_options(self, cf_name):
        # Returns the options for cf-name as a FullNamesOptionsDict
        full_options_names = []
        for section_type in self.options_dict:
            if cf_name in self.options_dict[section_type]:
                for option_name in self.options_dict[section_type][cf_name]:
                    full_option_name = \
                        get_full_option_name(section_type, option_name)
                    full_options_names.append(full_option_name)

        return self.get_options(full_options_names, cf_name)

    @staticmethod
    def get_unified_cfs_options(cfs_options):
        # dictionaries that will contain the results
        common_cfs_options = {}
        unique_cfs_options = {}

        if not cfs_options:
            return common_cfs_options, unique_cfs_options

        cfs_names = [cf_name for cf_name in cfs_options.keys()]

        # First assume no common options, later remove common
        # Remove cf-name from values to allow comparison
        for cf_name in cfs_names:
            assert isinstance(cfs_options[cf_name], FullNamesOptionsDict)
            unique_cfs_options[cf_name] = {}
            for option_name, option_value_with_cf_name in \
                    cfs_options[cf_name].options_dict.items():
                unique_cfs_options[cf_name][option_name] = \
                    option_value_with_cf_name[cf_name]

        # Check all options
        first_cf_name = cfs_names[0]
        for option_name in list(unique_cfs_options[first_cf_name].keys()):
            try:
                options_values =\
                    [unique_cfs_options[cf_name][option_name] for
                     cf_name in unique_cfs_options.keys()]
                different_options_values = set(options_values)
            except KeyError:
                # At least one cf doesn't have this option => not common
                continue

            if len(different_options_values) != 1:
                # At least one cf has a different value for this options =>
                # not common
                continue

            # The option is common to all cf-s => place in common
            # dict, and remove from all unique dicts
            common_cfs_options[option_name] = options_values[0]
            for cf_name in cfs_names:
                del(unique_cfs_options[cf_name][option_name])

        return common_cfs_options, unique_cfs_options

    @staticmethod
    def prepare_flat_full_names_cf_options_for_display(
            cf_options, option_value_prepare_func):
        if option_value_prepare_func is None:
            def option_value_prepare_func(value):
                return value

        options_for_display = {}
        table_options_for_display = {}

        for full_option_name, option_value in cf_options.items():
            section_type, option_name = \
                parse_full_option_name(full_option_name)

            if section_type == SectionType.CF:
                options_for_display[option_name] = \
                    option_value_prepare_func(option_value)

            else:
                assert section_type == SectionType.TABLE_OPTIONS
                table_options_for_display[option_name] = \
                    option_value_prepare_func(option_value)

        return options_for_display, table_options_for_display

    def get_cf_options_for_display(self, cf_name):
        options_for_display = {}
        table_options_for_display = {}
        cf_options = self.get_cf_options(cf_name)
        for full_option_name, option_value_with_cf in \
                cf_options.get_options_dict().items():
            option_value = option_value_with_cf[cf_name]
            section_type, option_name =\
                parse_full_option_name(full_option_name)

            if section_type == SectionType.CF:
                options_for_display[option_name] = option_value
            else:
                assert section_type == SectionType.TABLE_OPTIONS
                table_options_for_display[option_name] = option_value

        return options_for_display, table_options_for_display

    def get_cf_option(self, cf_name, option_name):
        # Returns the raw value of a single CF option
        full_option_name = get_full_option_name(SectionType.CF, option_name)
        option_value_dict = self.get_options([full_option_name], cf_name). \
            get_options_dict()
        if not option_value_dict:
            return None
        else:
            return option_value_dict[full_option_name][cf_name]

    def set_cf_option(self, cf_name, option_name, option_value,
                      allow_new_option=False):
        if not allow_new_option:
            assert self.get_cf_option(cf_name, option_name) is not None,\
                "Trying to update a non-existent CF Option." \
                f"cf:{cf_name} - {option_name} = {option_value}"

        self.create_section_and_cf_in_section_if_necessary(SectionType.CF,
                                                           cf_name)
        self.options_dict[SectionType.CF][cf_name][option_name] = \
            option_value

    def get_cf_table_option(self, cf_name, option_name):
        # Returns the raw value of a single CF Table option
        full_option_name = get_full_option_name(SectionType.TABLE_OPTIONS,
                                                option_name)
        option_value_dict = self.get_options([full_option_name], cf_name). \
            get_options_dict()
        if not option_value_dict:
            return None
        else:
            return option_value_dict[full_option_name][cf_name]

    def get_cf_table_raw_ptr_str(self, cf_name, options_name):
        sanitized_ptr = self.get_cf_table_option(cf_name, options_name)
        if sanitized_ptr is None:
            return None

        return sanitized_to_raw_ptr_value(sanitized_ptr)

    def set_cf_table_option(self, cf_name, option_name, option_value,
                            allow_new_option=False):
        if not allow_new_option:
            assert self.get_cf_table_option(cf_name, option_name) is not None,\
                "Trying to update a non-existent CF Table Option." \
                f"cf:{cf_name} - {option_name} = {option_value}"

        self.create_section_and_cf_in_section_if_necessary(
            SectionType.TABLE_OPTIONS, cf_name)
        self.options_dict[SectionType.TABLE_OPTIONS][cf_name][option_name] = \
            option_value

    def get_options(self, requested_full_options_names,
                    requested_cf_name=None):
        # The input is expected to be a set or a list of the format:
        # [<full option name>, ...]
        # Returns a FullNamesOptionsDict for the requested options
        assert isinstance(requested_full_options_names, list) or isinstance(
            requested_full_options_names, set),\
            f"Illegal requested_full_options_names type " \
            f"({type(requested_full_options_names)}"

        options = FullNamesOptionsDict()

        for full_option_name in requested_full_options_names:
            section_type, option_name = \
                parse_full_option_name(full_option_name)
            if section_type not in self.options_dict:
                continue
            if requested_cf_name is None:
                cf_names = self.options_dict[section_type].keys()
            else:
                cf_names = [requested_cf_name]

            for cf_name in cf_names:
                if cf_name not in self.options_dict[section_type]:
                    continue
                if option_name in self.options_dict[section_type][cf_name]:
                    option_value = \
                        self.options_dict[section_type][cf_name][
                            option_name]

                    options.set_option(section_type, cf_name, option_name,
                                       option_value)
        return options

    @staticmethod
    def get_options_diff(baseline, new):
        # Receives 2 sets of options and returns the difference between them.
        # Three cases exist:
        # 1. An option exists in the old but not in the new
        # 2. An option exists in the new but not in the old
        # 3. The option exists in both but the values differ
        # The inputs must be FullNamesOptionsDict instances. These may be
        # obtained
        # The resulting diff is an OptionsDiff with only actual
        # differences
        assert isinstance(baseline, FullNamesOptionsDict)
        assert isinstance(new, FullNamesOptionsDict)

        baseline_options = baseline.get_options_dict()
        new_options = new.get_options_dict()

        diff = OptionsDiff(baseline_options, new_options)

        full_options_names_union =\
            set(baseline_options.keys()).union(set(new_options.keys()))
        for full_option_name in full_options_names_union:
            # if option in options_union, then it must be in one of the configs
            if full_option_name not in baseline_options:
                for cf_name in new_options[full_option_name]:
                    diff.diff_in_new(cf_name, full_option_name)
            elif full_option_name not in new_options:
                for cf_name in baseline_options[full_option_name]:
                    diff.diff_in_base(cf_name, full_option_name)
            else:
                for cf_name in baseline_options[full_option_name]:
                    if cf_name in new_options[full_option_name]:
                        are_different = \
                            are_non_sanitized_values_different(
                                baseline_options[full_option_name][cf_name],
                                new_options[full_option_name][cf_name])
                        if are_different:
                            diff.diff_between(cf_name, full_option_name)
                    else:
                        diff.diff_in_base(cf_name, full_option_name)

                for cf_name in new_options[full_option_name]:
                    if cf_name in baseline_options[full_option_name]:
                        are_different = are_non_sanitized_values_different(
                            baseline_options[full_option_name][cf_name],
                            new_options[full_option_name][cf_name])
                        if are_different:
                            diff.diff_between(cf_name, full_option_name)
                    else:
                        diff.diff_in_new(cf_name, full_option_name)

        return diff if not diff.is_empty_diff() else None

    def get_options_diff_relative_to_me(self, new):
        baseline = self.get_all_options()
        return DatabaseOptions.get_options_diff(baseline, new)

    @staticmethod
    def get_cfs_options_diff(baseline, baseline_cf_name, new, new_cf_name):
        assert isinstance(baseline, FullNamesOptionsDict)
        assert isinstance(new, FullNamesOptionsDict)

        # Same as get_options_diff, but for specific column families.
        # This is needed to compare a parsed log file with a baseline version.
        # The baseline version contains options only for the default column
        # family. So, there is a need to compare the options for all column
        # families in the parsed log with the same default column family in
        # the base version
        baseline_opts_dict = baseline.get_options_dict()
        new_opts_dict = new.get_options_dict()

        diff = CfsOptionsDiff(baseline_opts_dict, baseline_cf_name,
                              new_opts_dict, new_cf_name)

        # Unify the names of the options in both, remove the duplicates, but
        # preserve the order of the options
        baseline_keys_list = list(baseline_opts_dict.keys())
        new_keys_list = list(new_opts_dict.keys())
        options_union = \
            utils.unify_lists_preserve_order(baseline_keys_list, new_keys_list)

        for full_option_name in options_union:
            # if option in options_union, then it must be in one of the configs
            if full_option_name not in baseline_opts_dict:
                new_option_values = new_opts_dict[full_option_name]
                if new_cf_name in new_option_values:
                    diff.diff_in_new(full_option_name)
            elif full_option_name not in new_opts_dict:
                baseline_option_values = baseline_opts_dict[full_option_name]
                if baseline_cf_name in baseline_option_values:
                    diff.diff_in_base(full_option_name)
            else:
                baseline_option_values = baseline_opts_dict[full_option_name]
                new_option_values = new_opts_dict[full_option_name]

                if baseline_cf_name in baseline_option_values:
                    if new_cf_name in new_option_values:
                        are_different = are_non_sanitized_values_different(
                            baseline_option_values[baseline_cf_name],
                            new_option_values[new_cf_name])
                        if are_different:
                            diff.diff_between(full_option_name)
                    else:
                        diff.diff_in_base(full_option_name)
                elif new_cf_name in new_option_values:
                    diff.diff_in_new(full_option_name)

        return diff if not diff.is_empty_diff() else None

    @staticmethod
    def get_db_wide_options_diff(opt_old, opt_new):
        return DatabaseOptions.get_cfs_options_diff(
            opt_old,
            DB_WIDE_CF_NAME,
            opt_new,
            DB_WIDE_CF_NAME)

    @staticmethod
    def get_unified_cfs_diffs(cfs_diffs):
        # Input [CfsOptionsDiff-1, CfsOptionsDiff-2...]

        # dictionaries that will contain the results
        common_cfs_diffs = {}
        unique_cfs_diffs = {}

        if not cfs_diffs:
            return common_cfs_diffs, unique_cfs_diffs

        # First assume no common options, later remove common
        # unique_cfs_diffs = copy.deepcopy(cfs_diffs)
        unique_cfs_diffs = copy.deepcopy(cfs_diffs)

        # Check all options diffs
        for option_name in list(unique_cfs_diffs[0].keys()):
            try:
                if option_name == CfsOptionsDiff.CF_NAMES_KEY:
                    continue
                option_diffs =\
                    [unique_cfs_diffs[cf_idx][option_name] for
                     cf_idx in range(len(unique_cfs_diffs))]
                different_options_diffs = set(option_diffs)
            except KeyError:
                # At least one cf doesn't have this option => not common
                continue

            if len(different_options_diffs) != 1:
                # At least one cf has a different diff for this option =>
                # not common
                continue

            # The option is common to all cf-s => place in common
            # dict, and remove from all unique dicts
            common_cfs_diffs[option_name] = option_diffs[0]
            for cf_diff in unique_cfs_diffs:
                del(cf_diff[option_name])

        return common_cfs_diffs, unique_cfs_diffs
