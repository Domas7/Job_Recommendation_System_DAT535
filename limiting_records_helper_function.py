import csv

def filter_csv_columns_and_limit_records(input_file, output_file, limit=1000000):
    
    # Open the input CSV file for reading
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        # Open the output CSV file for writing
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            row_count = 0
            for row in reader:
                if row_count >= limit:
                    break  # Stop after reaching the limit
                
                writer.writerow(row)
                row_count += 1

# Specify the input CSV file path and the output CSV file path
input_file = '/home/domas/Downloads/job_descriptions.csv'
output_file = '/home/domas/Downloads/job_descriptions_filtered.csv'

# Call the function to limit to 1 million records
filter_csv_columns_and_limit_records(input_file, output_file, limit=1000000)
