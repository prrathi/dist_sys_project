import csv

def create_subset_csv(source_file, dest_file, num_records):
    with open(source_file, 'r', newline='', encoding='utf-8') as infile, open(dest_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # skip the header row
        next(reader, None)  

        # write the specified number of data rows
        for i, row in enumerate(reader):
            if i >= num_records:
                break
            writer.writerow(row)

    print(f"created: {dest_file} with {num_records} records.")


source_csv = 'Traffic_Signs.csv'  

create_subset_csv(source_csv, 'TrafficSigns_1000.csv', 1000)
create_subset_csv(source_csv, 'TrafficSigns_5000.csv', 5000)
create_subset_csv(source_csv, 'TrafficSigns_10000.csv', 10000)