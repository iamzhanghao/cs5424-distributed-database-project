import csv

with open('project_files/data_files/order_combined.csv') as csv_file:
    with open('project_files/data_files/cleaned_order_combined.csv', 'w') as cleaned:

        csv_reader = csv.reader(csv_file, delimiter=',')
        writer = csv.writer(cleaned, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for row in csv_reader:
            if row[-4] == 'NULL':
                row[-4] = 'null'

            writer.writerow(row)