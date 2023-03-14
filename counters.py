import logging
import re

import regexes
import utils

format_err_msg = utils.format_err_msg
ParsingAssertion = utils.ParsingAssertion
ErrContext = utils.ErrorContext
format_line_num_from_entry = utils.format_line_num_from_entry
format_line_num_from_line_idx = utils.format_line_num_from_line_idx
get_line_num_from_entry = utils.get_line_num_from_entry


class CountersMngr:
    @staticmethod
    def is_start_line(line):
        return re.findall(regexes.STATS_COUNTERS_AND_HISTOGRAMS, line)

    @staticmethod
    def is_your_entry(entry):
        entry_lines = entry.get_msg_lines()
        return CountersMngr.is_start_line(entry_lines[0])

    def try_adding_entries(self, log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        entry = log_entries[entry_idx]

        if not CountersMngr.is_your_entry(entry):
            return False, entry_idx

        try:
            self.add_entry(entry)
        except utils.ParsingError:
            logging.error(f"Error while parsing Counters entry, Skipping.\n"
                          f"entry:{entry}")
        entry_idx += 1

        return True, entry_idx

    def __init__(self):
        # list of counters names in the order of their appearance
        # in the log file (retaining this order assuming it is
        # convenient for the user)
        self.counters_names = []
        self.counters = dict()
        self.histogram_counters_names = []
        self.histograms = dict()

    def add_entry(self, entry):
        time = entry.get_time()
        lines = entry.get_msg_lines()
        assert CountersMngr.is_start_line(lines[0])

        logging.debug(f"Parsing Counter and Histograms Entry ("
                      f"{format_line_num_from_entry(entry)}")

        for i, line in enumerate(lines[1:]):
            if self.try_parse_counter_line(time, line):
                continue
            if self.try_parse_histogram_line(time, line):
                continue

            # Skip badly formed lines
            logging.error(format_err_msg(
                "Failed parsing Counters / Histogram line"
                f"Entry. time:{time}",
                ErrContext(**{
                    "log_line_idx": get_line_num_from_entry(entry, i + 1),
                    "log_line": line})))

    def try_parse_counter_line(self, time, line):
        line_parts = re.findall(regexes.STATS_COUNTER, line)
        if not line_parts:
            return False
        assert len(line_parts) == 1 and len(line_parts[0]) == 2

        value = int(line_parts[0][1])
        counter_name = line_parts[0][0]
        if counter_name not in self.counters:
            self.counters_names.append(counter_name)
            self.counters[counter_name] = list()

        entries = self.counters[counter_name]
        if entries:
            prev_entry = entries[-1]
            prev_value = prev_entry["value"]

            if value < prev_value:
                logging.error(format_err_msg(
                    f"count or sum DECREASED during interval - Ignoring Entry."
                    f"prev_value:{prev_value}, count:{value}"
                    f" (counter:{counter_name}), "
                    f"prev_time:{prev_entry['time']}, time:{time}",
                    ErrContext(**{"log_line": line})))
                return True

        self.counters[counter_name].append({
            "time": time,
            "value": value})

        return True

    def try_parse_histogram_line(self, time, line):
        match = re.fullmatch(regexes.STATS_HISTOGRAM, line)
        if not match:
            return False
        assert len(match.groups()) == 7

        counter_name = match.group('name')
        count = int(match.group('count'))
        total = int(match.group('sum'))
        if total > 0 and count == 0:
            logging.error(format_err_msg(
                f"0 Count but total > 0 in a histogram (counter:"
                f"{counter_name}), time:{time}",
                ErrContext(**{"log_line": line})))

        if counter_name not in self.histograms:
            self.histograms[counter_name] = list()
            self.histogram_counters_names.append(counter_name)

        # There are cases where the count is > 0 but the
        # total is 0 (e.g., 'rocksdb.prefetched.bytes.discarded')
        if total > 0:
            average = float(f"{(total / count):.2f}")
        else:
            average = float(f"{0.0:.2f}")

        entries = self.histograms[counter_name]

        prev_count = 0
        prev_total = 0
        if entries:
            prev_entry = entries[-1]
            prev_count = prev_entry["values"]["Count"]
            prev_total = prev_entry["values"]["Sum"]

            if count < prev_count or total < prev_total:
                logging.error(format_err_msg(
                    f"count or sum DECREASED during interval - Ignoring Entry."
                    f"prev_count:{prev_count}, count:{count}"
                    f"prev_sum:{prev_total}, sum:{total},"
                    f" (counter:{counter_name}), "
                    f"prev_time:{prev_entry['time']}, time:{time}",
                    ErrContext(**{"log_line": line})))
                return True

        entries.append(
            {"time": time,
             "values": {"P50": float(match.group('P50')),
                        "P95": float(match.group('P95')),
                        "P99": float(match.group('P99')),
                        "P100": float(match.group('P100')),
                        "Count": count,
                        "Sum": total,
                        "Average": average,
                        "Interval Count": count - prev_count,
                        "Interval Sum": total - prev_total}})

        return True

    def does_have_counters_values(self):
        return self.counters != {}

    def does_have_histograms_values(self):
        return self.histograms != {}

    def get_counters_names(self):
        return self.counters_names

    def get_counters_times(self):
        all_entries = self.get_all_counters_entries()
        times = list(
            {counter_entry["time"]
             for counter_entries in all_entries.values()
             for counter_entry in counter_entries})
        times.sort()
        return times

    def get_counter_entries(self, counter_name):
        if counter_name not in self.counters:
            return {}
        return self.counters[counter_name]

    def get_non_zeroes_counter_entries(self, counter_name):
        counter_entries = self.get_counter_entries(counter_name)
        return list(filter(lambda entry: entry['value'] > 0,
                           counter_entries))

    def are_all_counter_entries_zero(self, counter_name):
        return len(self.get_non_zeroes_counter_entries(counter_name)) == 0

    def get_all_counters_entries(self):
        return self.counters

    def get_counters_entries_not_all_zeroes(self):
        result = {}

        for counter_name, counter_entries in self.counters.items():
            if not self.are_all_counter_entries_zero(counter_name):
                result.update({counter_name: counter_entries})

        return result

    def get_first_counter_entry(self, counter_name):
        entries = self.get_counter_entries(counter_name)
        if not entries:
            return {}
        return entries[0]

    def get_first_counter_value(self, counter_name, default=0):
        last_entry = self.get_first_counter_entry(counter_name)

        if not last_entry:
            return default

        return last_entry["value"]

    def get_last_counter_entry(self, counter_name):
        entries = self.get_counter_entries(counter_name)
        if not entries:
            return {}
        return entries[-1]

    def get_last_counter_value(self, counter_name, default=0):
        last_entry = self.get_last_counter_entry(counter_name)

        if not last_entry:
            return default

        return last_entry["value"]

    def get_histogram_counters_names(self):
        return self.histogram_counters_names

    def get_histogram_counters_times(self):
        all_entries = self.get_all_histogram_entries()
        times = list(
            {counter_entry["time"]
             for counter_entries in all_entries.values()
             for counter_entry in counter_entries})
        times.sort()
        return times

    def get_histogram_entries(self, counter_name):
        if counter_name not in self.histograms:
            return {}
        return self.histograms[counter_name]

    def get_all_histogram_entries(self):
        return self.histograms

    def get_last_histogram_entry(self, counter_name, non_zero):
        entries = self.get_histogram_entries(counter_name)
        if not entries:
            return {}
        last_entry = entries[-1]
        is_zero_entry_func = \
            CountersMngr.is_histogram_entry_count_zero
        if non_zero and is_zero_entry_func(last_entry):
            return {}

        return entries[-1]

    @staticmethod
    def is_histogram_entry_count_zero(entry):
        return entry['values']['Count'] == 0

    @staticmethod
    def is_histogram_entry_count_not_zero(entry):
        return entry['values']['Count'] > 0

    def get_non_zeroes_histogram_entries(self, counter_name):
        histogram_entries = self.get_histogram_entries(counter_name)
        return list(filter(lambda entry: entry['values']['Count'] > 0,
                           histogram_entries))

    def are_all_histogram_entries_zero(self, counter_name):
        return len(self.get_non_zeroes_histogram_entries(counter_name)) == 0

    def get_histogram_entries_not_all_zeroes(self):
        result = {}

        for counter_name, histogram_entries in self.histograms.items():
            if not self.are_all_histogram_entries_zero(counter_name):
                result.update({counter_name: histogram_entries})

        return result

    @staticmethod
    def get_histogram_entry_display_values(entry):
        disp_values = {}
        values = entry["values"]

        disp_values["Count"] = \
            utils.get_human_readable_number(values["Count"])
        disp_values["Sum"] = \
            utils.get_human_readable_number(values["Sum"])
        disp_values["Avg. Read Latency"] = f'{values["Average"]:.1f} us'
        disp_values["P50"] = f'{values["P50"]:.1f} us'
        disp_values["P95"] = f'{values["P95"]:.1f} us'
        disp_values["P99"] = f'{values["P99"]:.1f} us'
        disp_values["P100"] = f'{values["P100"]:.1f} us'

        return disp_values
