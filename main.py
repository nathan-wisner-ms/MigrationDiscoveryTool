import csv
import os

import psycopg2
import configparser

# Read the INI file and parse it into the connection string
config = configparser.ConfigParser()
config.read('config.INI')
host = config["source"]["host"]
port = config['source']['port']
dbname = config['source']['dbname']
user = config['source']['user']
password = config['source']['password']

# Variables
conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
getAllTableQueries = """SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public';"""
getAllDatabaseQuery = """SELECT datname FROM pg_database;"""
headerCsv = ["Table Name", "Missing Primary Keys?", "Table Size", "Collations", "Schema"]
validTableHeader = ['TableName', 'Size']
validDatabaseList = []
invalidDatabaseList = ['template1', 'template0', 'azure_maintenance', 'azure_sys']
invalidDataTypes = ['integer']

# Connecting to the DB from the string used and gathering all tables
conn = psycopg2.connect(conn_string)
crsr = conn.cursor()
crsr.execute(getAllDatabaseQuery)
databaseList = crsr.fetchall()

for database in databaseList:
    if not database[0] in invalidDatabaseList:
        validDatabaseList.append(database[0])


def connectToServer(dbName):
    local_conn_string = f"host={host} port={port} dbname={dbName} user={user} password={password}"
    # Connecting to the DB from the string used and gathering all tables
    db_conn = psycopg2.connect(local_conn_string)
    return db_conn.cursor()


def processDatabase(cursor, currentDb):
    db_cursor = cursor
    db_cursor.execute(getAllTableQueries)
    tableList = db_cursor.fetchall()
    reportRows = []
    validTableRows = []
    validTableList = []
    invalidTableList = ['pg_buffercache', 'pg_stat_statements']
    errorRows = []

    for table in tableList:
        if not table[0] in invalidTableList:
            validTableList.append(table[0])

    # For every table listed, lets iterate
    for table in validTableList:

        uniqueColumns = []
        columnToTypeDict = dict()
        # 3 Queries we use

        # Get all primary keys from a table
        getPrimaryKeyQuery = f"""SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = 
        '{table}'::regclass AND    i.indisprimary;"""

        # Get the size of a given table
        getTableSizeQuery = f"""SELECT pg_size_pretty (pg_relation_size('{table}'))"""

        # Get all collations in a given table
        getCollationsQuery = """select table_schema, table_name, column_name, collation_name
        from information_schema.columns where collation_name is not null order by table_schema, table_name, 
        ordinal_position;"""

        getSchemaQuery = """SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"""
        getDataTypeColumns = f"""SELECT column_name, data_type FROM information_schema.columns WHERE 
table_name = '{table}';"""

        # Done: Add schema to output
        # Done: Exclude extension names
        # TODO: Check listings, triggers, views
        # Done: Add checking databases under a server
        # Done: Remove  ('template1',), ('template0',), ('azure_maintenance',), ('azure_sys',) as databases

        # Execute all three queries and store the results in variables
        db_cursor.execute(getPrimaryKeyQuery)
        primaryKeyCount = len(db_cursor.fetchall())
        db_cursor.execute(getTableSizeQuery)
        tableSize = db_cursor.fetchall()
        db_cursor.execute(getCollationsQuery)
        collations = db_cursor.fetchall()
        db_cursor.execute(getSchemaQuery)
        schema = db_cursor.fetchall()
        db_cursor.execute(getDataTypeColumns)
        columns = db_cursor.fetchall()

        # For every column and data type, if we haven't seen that data type, record it and the column its associated
        # with
        for tuple in columns:
            if not tuple[1] in uniqueColumns:
                uniqueColumns.append(tuple[1])
                columnToTypeDict[tuple[1]] = tuple[0]

        # Record results to be written to 3 CSV files

        # Record data for the report CSV
        newCsvLine = [table, primaryKeyCount == 0, tableSize[0][0], collations, schema]
        reportRows.append(newCsvLine)

        # If we have at least one primary key, the table is valid, otherwise that is a fatal error we will write to the
        # ERROR.csv
        if primaryKeyCount > 0:
            validTableRows.append([table, tableSize[0][0]])
        else:
            errorRows.append(
                [f"FATAL ERROR, TABLE '{table}' has no primary keys, this DB CANNOT BE MIGRATED DUE TO TABLE "
                 f"{table}"])

        for invalidDataType in invalidDataTypes:
            if invalidDataType in uniqueColumns:
                errorRows.append(
                    [f"FATAL ERROR, TABLE {table} has invalid data types, this means these cannot be migrated.\n -"
                     f"The datatype that is not supported is '{invalidDataType}' in column "
                     f" '{columnToTypeDict[invalidDataType]}'"]
                )

    def createFolder(title):
        current_directory = os.getcwd()
        final_directory = os.path.join(current_directory, title)
        if not os.path.exists(final_directory):
            os.makedirs(final_directory)

    def writeCsv(header, rows, title):
        with open(title, 'w', newline='', encoding='UTF8') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)

    def writeErrorTxt():
        textfile = open("Errors/" + currentDb + "ERRORS.txt", "w")

        if len(errorRows) > 0:
            for element in errorRows:
                textfile.write(str(element[0]) + "\n")
        else:
            textfile.write("No errors to report, everything is ready for migration pending further review \n")
        textfile.close()

    createFolder('Analysis')
    createFolder('ValidTables')
    createFolder('Errors')
    writeCsv(headerCsv, reportRows, "Analysis/" + currentDb + "analysis.csv")
    writeCsv(validTableHeader, validTableRows, "ValidTables/" + currentDb + "validTables.csv")
    writeErrorTxt()


for database in validDatabaseList:
    cursor = connectToServer(database)
    processDatabase(cursor, database)
