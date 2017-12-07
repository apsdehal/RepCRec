# RepCRec

Replicated Concurrency Control and Recovery project for Advanced Database Class with Professor Shasha at NYU Fall 2017.


## Features

- Use multiversion concurrency control to read locks to transactions
- Deadlock detection and clearance in case of blocked transactions
- Replication of variables for better accessibility on websites
- Site failure recovery for transactions
- Uses available copies approach to mitigate failures and increase availability
- Supports read only transactions
- Configurable number of sites and variables
- Global transaction manager and per site data manager (containing lock table)
- Highly modular

## Running

- Run `pip install -r requirements.txt` to install all of the requirements using pip
- Copy config: `cp RepCRec/sample.config.py RepCRec/config.py`, edit it to set log level and other stuff
- Normal run command would `python -m RepCRec.start <input_file>`.
- Additionally, input can be provided via stdin through `-i` option.
- Sites can be brought up using `-s` option
- Similarly number of sites and variables are easily configurable, by default we have 10 sites and 20 variables in which even indexed are replicated on all sites and odd index are present on `(index + 1) % 10`   

```
$ python -m RepCRec.start --help

usage: start.py [-h] [-n 10] [-v 20] [-s] [-o None] [-i] file_path

positional arguments:
  file_path             File name, pass anything in case of stdin

optional arguments:
  -h, --help            show this help message and exit
  -n 10, --num-sites 10
                        Number of Sites
  -v 20, --num-variables 20
                        Number of variables
  -s, --sites           Whether to bring sites up
  -o None, --out-file None
                        Output file, if not passed by default we will print to
                        stdout
  -i, --stdin           Takes input from stdin instead of file if passed
```

## Design

Design can be found inside design folder of the repo.

## Tests

Various test have been added to support robust and care free development process. Run tests using following command:

> `python tests/run_tests.py`

This will provide you with results of various tests run and diffs of failures if any. Sample test case can be found in `tests/inputs` folder.

## Authors

- Amanpreet Singh [@apsdehal](https://github.com/apsdehal)
- Sharan Agrawal [@agr-shrn](https://github.com/agr-shrn)

## Acknowledgements

We would like to Acknowledge Prof. Dennis Shasha for his constant support, prompt question answering and lectures this semester.

## License
MIT
