from bson.objectid import ObjectId

from bson.code import Code

def exercise_1(col):
    result = None
    # Your solution here
    return result

def exercise_2(col):
    result = None
    # Your solution here
    return result

def process_exercise_3(result):
    process_result = {}
    for record in result:
        process_result[record['_id']['state']] = record['sum']/record['count']
    return process_result

def exercise_3(col,date1,date2):
    result = None
    # partial solution
    # Your solution here
    return result

def process_exercise_4(result):
    process_result = {}
    for record in result:
        state,identifier = record['_id'].split(": ")
        value = record['value']
        if state not in process_result:
            process_result[state] = 1.
        if identifier == "sum":
            process_result[state] *= value
        elif identifier == "count":
            process_result[state] *= 1/value
    return process_result

def exercise_4(col,date1,date2):
    result = None
    # partial solution
    # Your solution here
    return result


