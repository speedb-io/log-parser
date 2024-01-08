import logging

import regexes
import re
from dataclasses import dataclass
import utils


class MemRepParser:
    @dataclass
    class Report:
        arena_total: str = None
        arena_stats: dict = None
        cfs_total: str = None
        cfs_stats: dict = None
        misc_stats: dict = None

    @staticmethod
    def is_start_line(line):
        return re.fullmatch(regexes.MEM_REP_TITLE, line) is not None

    def __init__(self):
        self.reports = dict()

    def try_adding_entries(self, log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        entry = log_entries[entry_idx]
        mem_rep_lines = utils.remove_empty_lines_at_start(
            entry.get_msg_lines())

        if not mem_rep_lines:
            return False, entry_idx

        if not MemRepParser.is_start_line(mem_rep_lines[0]):
            return False, entry_idx

        try:
            logging.debug(f"Parsing Memory Report Entry ("
                          f"{utils.format_line_num_from_entry(entry)}")

            time = entry.get_time()
            self.add_lines(time, mem_rep_lines)
        except utils.ParsingError:
            logging.error(f"Error while parsing Memory Report entry, "
                          f"Skipping.\nentry:{entry}")
        entry_idx += 1

        return True, entry_idx

    def add_lines(self, time, mem_rep_lines):
        assert len(mem_rep_lines) > 0
        assert MemRepParser.is_start_line(mem_rep_lines[0])

        report = MemRepParser.Report()

        line_idx = 1
        arena_total, arena_section_stats, line_idx = \
            MemRepParser.parse_arena_stats_lines(mem_rep_lines, line_idx)
        report.arena_total = arena_total
        report.arena_stats = arena_section_stats

        cfs_total, cfs_section_stats, line_idx = \
            MemRepParser.parse_cfs_stats_lines(mem_rep_lines, line_idx)
        report.cfs_total = cfs_total
        report.cfs_stats = cfs_section_stats

        misc_section_stats, line_idx = \
            MemRepParser.parse_misc_stats_lines(mem_rep_lines, line_idx)
        report.misc_stats = misc_section_stats

        self.reports[time] = report

    def get_reports(self):
        return self.reports

    @staticmethod
    def parse_arena_stats_lines(mem_rep_lines, start_line_idx):
        line_idx = start_line_idx

        if not MemRepParser.is_arena_stats_title_line(mem_rep_lines[line_idx]):
            raise utils.ParsingError(
                f"Missing expected Arena Stats title. line:\n"
                f"{mem_rep_lines[line_idx]}")

        line_idx += 1
        total_match = MemRepParser.parse_total_line(mem_rep_lines[line_idx])
        if not total_match:
            raise utils.ParsingError(
                f"Missing expected Arena Total line. line:\n"
                f"{mem_rep_lines[line_idx]}")
        arena_total = total_match["usage"]

        line_idx += 1
        arena_section_stats, line_idx = \
            MemRepParser.parse_section_stats(mem_rep_lines, line_idx,
                                             regexes.MEM_REP_ENTITY_USAGE_LINE,
                                             "entity",
                                             regexes.MEM_REP_CFS_STATS_TITLE)

        return arena_total, arena_section_stats, line_idx

    @staticmethod
    def parse_cfs_stats_lines(mem_rep_lines, start_line_idx):
        line_idx = start_line_idx

        if not MemRepParser.is_cfs_stats_title_line(mem_rep_lines[line_idx]):
            raise utils.ParsingError(
                f"Missing expected CF-s Stats title. line:\n"
                f"{mem_rep_lines[line_idx]}")

        line_idx += 1
        total_match = MemRepParser.parse_total_line(mem_rep_lines[line_idx])
        if not total_match:
            raise utils.ParsingError(
                f"Missing expected CF-s Total line. line:\n"
                f"{mem_rep_lines[line_idx]}")
        cfs_total = total_match["usage"]

        line_idx += 1
        arena_section_stats, line_idx = \
            MemRepParser.parse_section_stats(mem_rep_lines, line_idx,
                                             regexes.MEM_REP_CF_USAGE_LINE,
                                             "cf")

        return cfs_total, arena_section_stats, line_idx

    @staticmethod
    def parse_misc_stats_lines(mem_rep_lines, start_line_idx):
        line_idx = start_line_idx
        return \
            MemRepParser.parse_section_stats(mem_rep_lines, line_idx,
                                             regexes.MEM_REP_ENTITY_USAGE_LINE,
                                             "entity")

    @staticmethod
    def parse_section_stats(mem_rep_lines, line_idx,
                            usage_line_regex,
                            key_name,
                            end_line_regex=None):
        section_stats = dict()
        while line_idx < len(mem_rep_lines):
            line = mem_rep_lines[line_idx].strip()

            # Finish when finding the specified end regex
            if end_line_regex:
                if re.fullmatch(end_line_regex, line) is not None:
                    break

            # But also finish when not finding another usage line
            match = re.fullmatch(usage_line_regex, line)
            if match is None:
                break

            section_stats[match[key_name]] = match["usage"]
            line_idx += 1

        return section_stats, line_idx

    @staticmethod
    def parse_total_line(line):
        return re.fullmatch(regexes.MEM_REP_TOTAL_LINE, line)

    @staticmethod
    def is_total_line(line):
        return MemRepParser.parse_total_line(line) is not None

    @staticmethod
    def is_arena_stats_title_line(line):
        return re.fullmatch(regexes.MEM_REP_ARENA_STATS_TITLE, line) is not \
               None

    @staticmethod
    def is_cfs_stats_title_line(line):
        return re.fullmatch(regexes.MEM_REP_CFS_STATS_TITLE, line) is not \
               None
