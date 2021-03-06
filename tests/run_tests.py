import subprocess
import os
from os import path
from termcolor import colored
import difflib

INPUT_FOLDER = 'tests/inputs/'
OUTPUT_FOLDER = 'tests/outputs/'
INT_FILE = 'output'


def color_diff(diff):
    for line in diff:
        if line.startswith('+'):
            yield colored(line, "green")
        elif line.startswith('-'):
            yield colored(line, "red")
        elif line.startswith('^'):
            yield colored(line, "blue")
        else:
            yield line


def main():
    input_files = os.listdir(INPUT_FOLDER)
    output_files = os.listdir(OUTPUT_FOLDER)

    input_files.sort()
    output_files.sort()

    d = difflib.Differ()

    for inp_file, out_file in zip(input_files, output_files):
        os.system('python -m RepCRec.start -o ' + INT_FILE + ' ' +
                  INPUT_FOLDER + str(inp_file))

        actual_output = []
        expected_output = str()
        stripped_expected_output = []

        with open(INT_FILE, 'r') as outfile, \
                open(OUTPUT_FOLDER + str(out_file), 'r') as test_file:

            result = outfile.readlines()
            expected_output = test_file.readlines()
            for line1, line2 in zip(result, expected_output):

                line1 = line1.split('-')[-1].strip()
                actual_output.append(line1)
                line2 = line2.split("-")[-1].strip()
                stripped_expected_output.append(line2)

        diff = difflib.ndiff(actual_output, stripped_expected_output)
        diff_final = []
        for d in diff:
            if d.startswith('+') or d.startswith('-'):
                diff_final.append(d)
        diff = diff_final
        print("\n".join(color_diff(diff)))

        if actual_output == stripped_expected_output:
            print(colored(str(inp_file) + ": Success", "green"))
        else:
            print(colored(str(inp_file) + ": Failed", "red"))
    os.remove(INT_FILE)


if __name__ == '__main__':
    main()
