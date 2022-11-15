import csv

# def log_correction(c_file):
#     new_file = c_file.split(".")[0] + "_1.log"
#     with open(c_file) as f:
#         reader = csv.reader(f)
#         data = list(reader)
#         flat_list = [x for xs in data for x in xs]
#         for i in range(len(flat_list)):
#             flat_list[i] = str(flat_list[i]).replace('|"', '| "')
#     with open(new_file, "w", newline="") as nf:
#         write = csv.writer(nf)
#         for line in flat_list:
#             write.writerow([line])
