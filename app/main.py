#!/bin/python3

import datetime
from ast import Load
import json
from types import SimpleNamespace
from os.path import exists
import sys
import pyodbc

# Background Colors to log messages
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def Log(message):
    print(f"{bcolors.BOLD}{bcolors.OKBLUE}[{datetime.datetime.now()}] {bcolors.OKCYAN}{message}")

def LogError(message):
    print(f"{bcolors.BOLD}{bcolors.OKBLUE}[{datetime.datetime.now()}] {bcolors.FAIL}{message}")    

def LogOk(message):
    print(f"{bcolors.BOLD}{bcolors.OKBLUE}[{datetime.datetime.now()}] {bcolors.OKGREEN}{message}")        

# Read task json task file
def LoadFromFile(path):
    # Opening JSON file
    f = open(path)
    
    # making the object              
    task = json.loads(f.read(), object_hook=lambda d: SimpleNamespace(**d))
            
    # Closing file
    f.close()        

    return task

# get command line args
Log("Received args:")
for a in sys.argv:
    Log("\t{0}".format(a))

n = len(sys.argv)

if n < 2:
    LogError("We need json task file path to continue")
    sys.exit(1)

# Get the last arg as a json task file
taskFile = sys.argv[len(sys.argv)-1] 
LogOk('Using {0} as Json Task File'.format(taskFile))

# check if file exists
if not exists(taskFile):
    LogError("Cannot find {0} file".format(taskFile))
    sys.exit(1)

tasks = LoadFromFile(taskFile)

# connect on db
Log('Connection on database...')
connStr = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={tasks.server};DATABASE={tasks.database};UID={tasks.username};PWD={tasks.password}"
Log(f"Connection string: {connStr}")
conn = pyodbc.connect(connStr)

LogOk('Connected!')

for t in tasks.tasks:
    Log('\tRunning query from {0}...'.format(t.queryFile))

    # Read query file
    if not exists(t.queryFile):
        LogError(f"\t{t.queryFile} query file not found!")
        sys.exit(1)

    f = open(t.queryFile, "r")
    query = f.read()

    Log(f"\tQuery to execute:\n{query}")
    # Get curson from connection
    cursor = conn.cursor()
    
    # execute queries
    cursor.execute(query)

    # Create header
    header = ""
    for c in cursor.description:
        header += c[0] + ";"
    
    Log(f"\tHeader: {header}")
    header += "\n"

    # Get first line
    row = cursor.fetchone()     
    data = ""
    
    # Interate cursor and get data
    while row:
        for c in row:
            data += f"{c};"

        Log(f"\tLine read: {data}")
        data += "\n"
        row = cursor.fetchone()

    # close cursor
    cursor.close()

    with open(t.outputPath + t.outputFilename, "w") as text_file:        
        text_file.write(header)
        text_file.write(data)

    LogOk(f"\tFile created on {t.outputPath + t.outputFilename}")

# close db connection
conn.close()
LogOk("Connection closed")

# Return with no error
LogOk('Done!')
sys.exit(0)