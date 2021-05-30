import sys

# FILE LOCATIONS #

input_file_location = "inputs/twitter_rv_orig.net"
output_header_location = "inputs/rels-header.csv"
output_file_location = "inputs/rels.csv"


input_file = open(input_file_location, "r")
output_file = open(output_file_location, "w")
output_header_file = open(output_header_location, "w")

# ITERATE INPUT FILE #
print(" === BEGIN PARSING === ")
num_viewed = 0
for current_line in input_file:
    current_ids = current_line.rstrip().split('\t')
    output_file.write("user" + current_ids[1] + ",user" + current_ids[0] + "\n")
    num_viewed = num_viewed + 1
    if num_viewed % 100000000 == 0:
        print("\tParsed " + str(num_viewed) + " lines so far")

output_header_file.write(":START_ID,:END_ID\n")

input_file.close()
output_file.close()
output_header_file.close()
