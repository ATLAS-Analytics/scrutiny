from datetime import datetime
from uuid import uuid4

from org.apache.pig.scripting import *

@outputSchema("uuid:chararray")
def UUID():
    return str(uuid4()).replace('-', '').lower()

@outputSchema("type:chararray")
def getDatatype(file):
    splits = file.split('.')
    if file.startswith('data') or file.startswith('mc'):
        if len(splits) < 5:
            type = file
        else:
            type = splits[4].split('_')[0]
    else:
        type = splits[0].split('_')[0]
    return type

@outputSchema("type:chararray")
def getDAODType(file):
    items = file.split('.')[0].split('_')
    if len(items) >= 2:
        return items[1]
    else:
        return 'None'

@outputSchema("date:chararray")
def toDay(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

@outputSchema("type:chararray")
def getFilename(path):
    items = path.strip().split('/')
    did = items[-1].split(':')
    filename = did[-1]
    return filename

@outputSchema("type:chararray")
def getProjectDatatype(name):
    items = name.strip().split('.')
    ret = items[0] + '_' + items[4]
    return ret

@outputSchema("type:chararray")
def getTraceType(id):
    if id is None:
        return 'local'
    else:
        return 'grid'
