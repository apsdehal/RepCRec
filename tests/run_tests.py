import subprocess
import os
from os import path

# input_files = [f for f in os.listdir('inputs/') if path.isfile(f)]
# output_files = [f for f in os.listdir('outputs/') if path.isfile(f)]

input_files = os.listdir('tests/inputs/')
output_files = os.listdir('tests/outputs/')

input_files.sort()
output_files.sort()


for inp_file, out_file in zip(input_files, output_files):

    os.system('python -m RepCRec.start tests/inputs/' + str(inp_file))

    my_output = str()
    test_output = str()
    new_test_output = str()

    with open('my_output.txt', 'r') as outfile, open('tests/outputs/' + str(out_file), 'r') as myfile:

        result = outfile.readlines()
        test_output = myfile.readlines()

        for line1, line2 in zip(result, test_output):

            my_output += line1.split('-')[1]
            new_test_output += line2.split("-")[1]

            if my_output != new_test_output:
                print(line1, line2)

    if my_output == new_test_output:
        print("Success")
    else:
        print("Failed " + str(inp_file))
