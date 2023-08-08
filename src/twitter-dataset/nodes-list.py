import sys

# FILE LOCATIONS #

input_file_location = "inputs/twitter_rv_orig.net"
output_header_location = "inputs/users-header.csv"
output_file_location = "inputs/users.csv"


input_file = open(input_file_location, "r")
output_file = open(output_file_location, "w")
output_header_file = open(output_header_location, "w")

# ITERATE INPUT FILE #
print(" === BEGIN PARSING === ")
ids = set()
num_viewed = 0
for current_line in input_file:
    current_ids = current_line.rstrip().split('\t')
    ids.add(int(current_ids[0]))
    ids.add(int(current_ids[1]))
    num_viewed = num_viewed + 1
    if num_viewed % 100000000 == 0:
        print ("\tParsed " + str(num_viewed) + " lines so far")

print(" === BEGIN SORTING === ")
# SORT FILE #
sorted_ids = list(ids)
sorted_ids.sort()
 
# OUTPUT TO FILES #
print(" === BEGIN OUTPUT === ")
output_header_file.write("identifier:ID,userId:int\n")

num_output = 0
for i in sorted_ids:
    output_file.write("user" + str(i) + "," + str(i) + "\n")
    num_viewed = num_viewed + 1
    if num_viewed % 10000000 == 0:
        print ("\tOutput " + str(num_output) + " lines so far")

input_file.close()
output_file.close()
output_header_file.close()
