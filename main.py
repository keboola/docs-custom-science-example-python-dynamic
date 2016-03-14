import csv
from keboola import docker

# initialize application
cfg = docker.Config('/data/')

# get list of input tables
tables <- app.getInputTables()
for (table in tables) {
    # get csv file name 
    inName <- table['destination'] 
    
    # read input table metadata
    manifest <- app.getTableManifest(name)

    # get csv file name with full path from output mapping
    outName <- app.getExpectedOutputTables()[table]['full_path']

    # get file name from output mapping
    outDestination <- app.getExpectedOutputTables()[table]['destination']

    # get csv full path and read table data
    i = 0
    with open(inName, mode='rt', encoding='utf-8') as inFile, open(outName, mode='wt', encoding='utf-8') as outFile:
        # read input file line-by-line
        lazyLines = (line.replace('\0', '') for line in inFile)
        csvReader = csv.DictReader(lazyLines, dialect='kbc')
        headers = reader.fieldnames

        # write output file header
        writer = csv.DictWriter(outFile, fieldnames = headers.extend('primaryKey'), dialect='kbc')
        writer.writeheader()

        for row in csvReader:
            # if there is no primary key
            if (len(manifest['primary_key']) == 0):
                i = i + 1
                row['primaryKey'] = i
            else:
                row['primaryKey'] = NULL
   
            writer.writerow(row)
   
    if (len(manifest['primary_key']) == 0):
        pk = ['primaryKey']
    else:
        pk = manifest['primary_key']

    # write table metadata - set new primary key
    app.writeTableManifest(outName, destination = outDestination, primaryKey = pk)
}