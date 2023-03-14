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

import argparse
import io
import logging
import logging.config
import os
import pathlib
import shutil
import sys
import textwrap
import threading
import time
from pathlib import Path

import console_outputter
import csv_outputter
import json_outputter
import utils
from log_file import ParsedLog

DEBUG_MODE = True

# event to signal the process bar thread to exit
exit_event = threading.Event()


def exit_program(exit_code):
    exit_event.set()
    exit(exit_code)


def display_process_bar():
    # Simple textual process bar that starts "playing" once parsing
    # exceeds 1 second
    time_from_last_print = 0

    line = "Parsing "
    while True:
        time.sleep(0.05)
        if exit_event.is_set():
            break
        time_from_last_print += 50
        if time_from_last_print >= 1000:
            line += "."
            print(line, end='\r')
            sys.stdout.flush()
            time_from_last_print = 0


def parse_log(log_file_path):
    if not os.path.isfile(log_file_path):
        raise utils.LogFileNotFoundError(log_file_path)

    logging.debug(f"Parsing {log_file_path}")
    with open(log_file_path) as log_file:
        logging.debug(f"Starting to read the contents of {log_file_path}")
        log_lines = log_file.readlines()
        logging.debug(f"Completed reading the contents of {log_file_path}")

        return ParsedLog(log_file_path, log_lines,
                         should_init_baseline_info=True)


def setup_cmd_line_parser():
    epilog = textwrap.dedent('''\
    Notes:
    - The default it to print to the console in a short format.
    - It is possible to specify both json and console outputs. Both will be generated.
    ''')  # noqa

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog)
    parser.add_argument("log_file_path",
                        metavar="log-file-path",
                        help="A path to a log file to parse "
                             "(default: %(default)s)")
    parser.add_argument("-c", "--console",
                        choices=["short", "long"],
                        help="Print to console a summary (short) or a "
                             "detailed (long) output (default: %(default)s)")
    parser.add_argument("-j", "--generate-json",
                        action="store_true",
                        default=False,
                        help=f"Optionally generate a JSON file in the "
                             f"output folder with detailed information. "
                             f"If generated, it will be called "
                             f"{utils.DEFAULT_JSON_FILE_NAME}."
                             f"(default: %(default)s)")
    parser.add_argument("-o", "--output-folder",
                        default=utils.DEFAULT_OUTPUT_FOLDER,
                        help="The name of the folder where output "
                             "files will be stored in SUB-FOLDERS "
                             f"named "
                             f"{utils.OUTPUT_SUB_FOLDER_PREFIX}dddd."
                             "'dddd' is the run number (default: %(default)s)")
    parser.add_argument("-l", "--generate-log",
                        action="store_true",
                        default=False,
                        help="Generate a log file for the parser's log "
                             "messages (default: %(default)s)")
    return parser


def validate_and_sanitize_cmd_line_args(cmdline_args):
    # The default is short console output
    if not cmdline_args.console and not cmdline_args.generate_json:
        cmdline_args.console = utils.ConsoleOutputType.SHORT


def handle_exception(exception, console, should_exit):
    logging.exception(f"\n{exception}")
    if console:
        if hasattr(exception, 'msg'):
            print(exception.msg, file=sys.stderr)
        else:
            print(exception, file=sys.stderr)
    if should_exit:
        exit_program(1)


def report_exception(exception, console):
    handle_exception(exception, console, should_exit=True)


def fatal_exception(exception, console=True):
    handle_exception(exception, console, should_exit=True)


def verify_min_python_version():
    if sys.version_info.major < utils.MIN_PYTHON_VERSION_MAJOR or \
            sys.version_info.minor < utils.MIN_PYTHON_VERSION_MINOR:
        msg = f"The log parser tool requires Python Version >= " \
              f"{utils.MIN_PYTHON_VERSION_MAJOR}." \
              f"{utils.MIN_PYTHON_VERSION_MINOR} " \
              f"(Yours is {sys.version_info.major}.{sys.version_info.minor})"
        print(msg, file=sys.stderr)
        exit_program(1)


def setup_logger(generate_log, output_folder):
    if not generate_log:
        logging.root.setLevel(logging.FATAL)
        return

    my_log_file_path = utils.get_log_file_path(output_folder)
    logging.basicConfig(filename=my_log_file_path,
                        format="%(asctime)s - %(levelname)s [%(filename)s "
                               "(%(funcName)s:%(lineno)d)] %(message)s)",
                        level=logging.DEBUG)
    return my_log_file_path


def prepare_output_folder(output_folder_parent):
    output_path_parent = pathlib.Path(output_folder_parent)
    largest_num = 0
    if output_path_parent.exists():
        for file in output_path_parent.iterdir():
            name = file.name
            if name.startswith(utils.OUTPUT_SUB_FOLDER_PREFIX):
                name = name[len(utils.OUTPUT_SUB_FOLDER_PREFIX):]
                if name.isnumeric() and len(name) == 4:
                    num = int(name)
                    largest_num = max(largest_num, num)

        if largest_num == 9999:
            largest_num = 1

    output_folder = f"{output_folder_parent}/" \
                    f"{utils.OUTPUT_SUB_FOLDER_PREFIX}" \
                    f"{largest_num + 1:04}"
    if not output_path_parent.exists():
        os.makedirs(output_folder_parent)
    shutil.rmtree(output_folder, ignore_errors=True)
    os.makedirs(output_folder)
    return output_folder


def print_to_console_if_applicable(cmdline_args, log_file_path,
                                   parsed_log, json_content):
    f = io.StringIO()
    console_content = None
    if cmdline_args.console == utils.ConsoleOutputType.SHORT:
        f.write(
            console_outputter.get_console_output(
                log_file_path,
                parsed_log,
                cmdline_args.console))
        console_content = f.getvalue()
    elif cmdline_args.console == utils.ConsoleOutputType.LONG:
        console_content = json_outputter.get_json_dump_str(json_content)

    if console_content is not None:
        print()
        print(console_content)


def generate_json_if_applicable(
        cmdline_args, parsed_log, csvs_paths, output_folder,
        report_to_console):
    json_content = None
    if cmdline_args.generate_json or \
            cmdline_args.console == utils.ConsoleOutputType.LONG:
        json_content = json_outputter.get_json(parsed_log)
        json_content["CSV-s"] = csvs_paths

        if cmdline_args.generate_json:
            json_file_name = utils.DEFAULT_JSON_FILE_NAME
            json_outputter.write_json(json_file_name,
                                      json_content, output_folder,
                                      report_to_console)

    return json_content


def generate_csvs_if_applicable(parsed_log, output_folder, report_to_console):
    assert isinstance(parsed_log, ParsedLog)

    cfs_names = parsed_log.get_cfs_names(include_auto_generated=False)
    events_mngr = parsed_log.get_events_mngr()
    stats_mngr = parsed_log.get_stats_mngr()
    compactions_monitor = parsed_log.get_compactions_monitor()

    counters_mngr = \
        parsed_log.get_counters_mngr()
    counters_csv_path = csv_outputter.generate_counters_csv(
        counters_mngr, output_folder, report_to_console)
    human_readable_histograms_csv_file_path, tools_histograms_csv_file_path = \
        csv_outputter.generate_histograms_csv(
            counters_mngr, output_folder, report_to_console)

    compaction_stats_mngr = stats_mngr.get_compactions_stats_mngr()
    compactions_stats_csv_path = csv_outputter.generate_compactions_stats_csv(
        compaction_stats_mngr, output_folder, report_to_console)

    compactions_csv_path = csv_outputter.generate_compactions_csv(
        compactions_monitor, output_folder, report_to_console)

    flushes_csv_path = csv_outputter.generate_flushes_csv(
        cfs_names, events_mngr, output_folder, report_to_console)

    def generate_disp_path(path):
        if path is None:
            return utils.FILE_NOT_GENERATED_TEXT

        assert isinstance(path, Path)
        return str(path)

    return {
        "Counters": generate_disp_path(counters_csv_path),
        "Histograms (Human-Readable)":
            generate_disp_path(human_readable_histograms_csv_file_path),
        "Histograms (Tools)":
            generate_disp_path(tools_histograms_csv_file_path),
        "Compactions-Stats": generate_disp_path(compactions_stats_csv_path),
        "Compactions": generate_disp_path(compactions_csv_path),
        "Flushes": generate_disp_path(flushes_csv_path)
    }


def main():
    verify_min_python_version()
    parser = setup_cmd_line_parser()
    cmdline_args = parser.parse_args()

    output_folder = cmdline_args.output_folder

    output_folder = prepare_output_folder(output_folder)
    my_log_file_path = setup_logger(cmdline_args.generate_log,
                                    output_folder)
    validate_and_sanitize_cmd_line_args(cmdline_args)

    t = threading.Thread(target=display_process_bar)
    t.start()

    try:
        log_file_path = cmdline_args.log_file_path
        log_file_path = os.path.abspath(log_file_path)
        parsed_log = parse_log(log_file_path)

        exit_event.set()

        if cmdline_args.console == utils.ConsoleOutputType.LONG:
            report_to_console = False
        else:
            report_to_console = True

        if report_to_console:
            log_file_path_str = parsed_log.get_log_file_path()
            print(f"Log file: {str(Path(log_file_path_str).as_uri())}")

            baseline_info = parsed_log.get_baseline_info()
            if baseline_info is not None:
                print(f"Baseline Log: "
                      f"{str(baseline_info.baseline_log_path.as_uri())}")
            else:
                print("No Available Baseline Log")

        csvs_paths = generate_csvs_if_applicable(parsed_log, output_folder,
                                                 report_to_console)
        json_content = generate_json_if_applicable(
            cmdline_args, parsed_log, csvs_paths, output_folder,
            report_to_console)
        print_to_console_if_applicable(cmdline_args, log_file_path, parsed_log,
                                       json_content)
    except utils.ParsingError as exception:
        report_exception(exception, console=True)
    except utils.LogFileNotFoundError as exception:
        report_exception(exception, console=True)
    except utils.EmptyLogFile as exception:
        report_exception(exception, console=True)
    except utils.InvalidLogFile as exception:
        report_exception(exception, console=True)
    except ValueError as exception:
        fatal_exception(exception)
    except AssertionError as exception:
        if not DEBUG_MODE:
            fatal_exception(exception)
        else:
            exit_event.set()
            raise
    except Exception as exception:  # noqa
        if not DEBUG_MODE:
            print(f"An unrecoverable error occurred while parsing "
                  f"{log_file_path}.", file=sys.stderr)
            print("Please open an issue in the tool's GitHub repository "
                  "(https://github.com/speedb-io/log-parser)", file=sys.stderr)
            if my_log_file_path:
                print(f"\nMore details may be found in {my_log_file_path}",
                      file=sys.stderr)

            fatal_exception(exception, console=False)
        else:
            exit_event.set()
            raise


if __name__ == '__main__':
    main()
