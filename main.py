import csv
from keboola import docker

# initialize cfglication
cfg = docker.Config('/data/')

# get list of input tables
tables = cfg.get_input_tables()
j = 0
for table in tables:
    # get csv file name
    inName = table['destination']

    # read input table metadata
    manifest = cfg.get_table_manifest(inName)

    # get csv file name with full path from output mcfging
    outName = cfg.get_expected_output_tables()[j]['full_path']

    # get file name from output mcfging
    outDestination = cfg.get_expected_output_tables()[j]['destination']

    # get csv full path and read table data
    i = 0
    with open(table['full_path'], mode='rt', encoding='utf-8') as in_file, open(outName, mode='wt', encoding='utf-8') as out_file:
        # read input file line-by-line
        lazy_lines = (line.replace('\0', '') for line in in_file)
        csvReader = csv.DictReader(lazy_lines, dialect='kbc')
        headers = csvReader.fieldnames
        headers.extend(['primaryKey'])

        # write output file header
        writer = csv.DictWriter(out_file, fieldnames=headers, dialect='kbc')
        writer.writeheader()

        for row in csvReader:
            # if there is no primary key
            if (len(manifest['primary_key']) == 0):
                i = i + 1
                row['primaryKey'] = i
            else:
                row['primaryKey'] = None

            writer.writerow(row)

    if (len(manifest['primary_key']) == 0):
        pk = ['primaryKey']
    else:
        pk = manifest['primary_key']

    # write table metadata - set new primary key
    cfg.write_table_manifest(outName, destination=outDestination, primary_key=pk)
    j = j + 1
