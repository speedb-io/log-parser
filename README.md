## Speedb-Log-Parser: A tool for parsing RocksDB and Speedb Log Files

<!-- ABOUT THE PROJECT -->
## About The Project
Speedb's Log Parser is a tool that may be used to parse and process 
Speedb and RocksDB log files.

The tool extracts useful information from these logs and aids users in 
gaining insights about their systems with ease. It is expected to be a 
valuable tool for novices and experts alike.

Detailed documentation for the log parser tool is available at 
https://speedb.atlassian.net/l/cp/3mKT4gYH

TODO: - Update this to the GitBook URL when it is available under https://github.com/speedb-io/documentation/tree/main/tools


<!-- GETTING STARTED -->
## Getting Started
The tool runs on a single log file and generates one or more outputs:
1. A short summary printed to the console
2. A detailed summary in a JSON format
3. A detailed summary printed to the console. This is effectively the JSON 
   file. This output may be filtered by command-line tools such 
   as JQ (https://stedolan.github.io/jq/).
4. A CSV file with the statistics counters (if available): `counters.csv`
5. A CSV(s) file with the statistics histograms counters (if available). 
   There are 2 such files generated:
   1. `histograms_human_readable.csv`: Aimed at humans (easier to read
      by humans).
   2. `histograms_tools.csv`: Aimed at automated tools.
6. A CSV file with the compaction statistics: `compaction.csv`
7. A log file of the tool's run (used for debugging purposes)

By default, a short console summary will be displayed. Users may request the 
tool to also generate the JSON detailed summary, and the detailed console 
summary.

The outputs are placed in a sub-folder under a separate parent folder. The 
user may specify the name of the parent folder(The default name for the 
parent folder is `output_files`). 
The naming conventions for the sub-folders is: "log_parser_XXXX", where 
XXXX are 4 digits. A new sub-folder is created every run, and the number (XXXX) 
increases by 1. The numbers wrap-around when reaching 9999.

Running the tool without any parameters will allow users to view the 
possible flags the tool supports:
   ```sh
   python3 log_parser.py
   ```

And also get detailed help information:
   ```sh
   python3 log_parser.py -h
   ```


### Prerequisites

Python version 3.8 or greater

### Installation

1. Clone the repo
   ```sh
   git clone git@github.com:speedb-io/log-parser.git
   ```

2. pytest installation (only if you wish / need to run the pytest unit 
   tests under the test folder)
   ```sh
   pip install pytest
   ```
3. flake8 (only if you modify any files or add new ones)
   ```sh
   pip install flake8
   ```

### Testing
The tool comes with a set of pytest unit tests. The unit tests are run by 
the pytest unit testing framework (https://docs.pytest.org).

To run the unit tests:
1. Install pytest (see Installaion section for details)
2. Go to the test folder
   ```sh
   cd test
   ```

3. Run the tests
   ```sh
   pytest
   ```
All the unit tests should pass.

<!-- USAGE EXAMPLES -->
## Usage

The repo contains a sample file that may be used to run the tool:
   ```sh
   python3 log_parser.py test/input_files/LOG_speedb
   ```




<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Add / Modify / Update unit tests to the test folder as applicable
4. Verify that all existing and new unit tests pass:
   ```sh
   cd test
   pytest
   ```
5. Verify that all the parser's python scripts and test scripts cleanly 
   pass the flake8 verification tool:
   ```sh
   flake8 *.py
   flake8 test/*.py
   ```
6. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
7. Push to the Branch (`git push origin feature/AmazingFeature`)
8. Open a Pull Request


<!-- LICENSE -->
## License

Distributed under the Apache V2 License. See `LICENSE.txt` for more information.



<!-- CONTACT -->
## Contact

TBD
