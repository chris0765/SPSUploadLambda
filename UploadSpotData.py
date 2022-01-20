import boto3
import time
import pandas as pd
from tqdm import tqdm
from botocore.config import Config

Datas = pd.read_csv("./1123-1209-join.csv")

DATABASE_NAME = "Spot_Database_updatetest"
TABLE_NAME = "Spot_Table_updatetest"
KeyId = "f39031bc-e1c3-4ebf-8e74-8dbf2e1e9025"
HT_TTL_HOURS = 8766
CT_TTL_DAYS = 3650

session = boto3.Session()

write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

query_client = session.client('timestream-query')

# Create Database

print("Creating Database")
try:
    write_client.create_database(DatabaseName=DATABASE_NAME)
    print("Database [%s] created successfully." % DATABASE_NAME)
except write_client.exceptions.ConflictException:
    print("Database [%s] exists. Skipping database creation" % DATABASE_NAME)
except Exception as err:
    print("Create database failed:", err)

# Describe Database

print("Describing database")
try:
    result = write_client.describe_database(DatabaseName=DATABASE_NAME)
    print("Database [%s] has id [%s]" % (DATABASE_NAME, result['Database']['Arn']))
except write_client.exceptions.ResourceNotFoundException:
    print("Database doesn't exist")
except Exception as err:
    print("Describe database failed:", err)

# Update database

print("Updating database")
try:
    result = write_client.update_database(DatabaseName=DATABASE_NAME, KmsKeyId=KeyId)
    print("Database [%s] was updated to use kms [%s] successfully" % (DATABASE_NAME, result['Database']['KmsKeyId']))
except write_client.exceptions.ResourceNotFoundException:
    print("Database doesn't exist")
except Exception as err:
    print("Update database failed:", err)

def delete_table():
    print("Deleting Table")
    try:
        result = write_client.delete_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME)
        print("Delete table status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except write_client.exceptions.ResourceNotFoundException:
        print("Table [%s] doesn't exist" % TABLE_NAME)
    except Exception as err:
        print("Delete table failed:", err)

delete_table()

# Create table

print("Creating table")
retention_properties = {
    'MemoryStoreRetentionPeriodInHours': HT_TTL_HOURS,
    'MagneticStoreRetentionPeriodInDays': CT_TTL_DAYS
}
try:
    write_client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, RetentionProperties=retention_properties)
    print("Table [%s] successfully created." % TABLE_NAME)
except write_client.exceptions.ConflictException:
    print("Table [%s] exists on database [%s]. Skipping table creation" % (TABLE_NAME, DATABASE_NAME))
except Exception as err:
    print("Create table failed:", err)

# Describe table

print("Describing table")
try:
    result = write_client.describe_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME)
    print("Table [%s] has id [%s]" % (TABLE_NAME, result['Table']['Arn']))
except write_client.exceptions.ResourceNotFoundException:
    print("Table doesn't exist")
except Exception as err:
    print("Describe table failed:", err)

# Update table

print("Updating table")
retention_properties = {
    'MemoryStoreRetentionPeriodInHours': HT_TTL_HOURS,
    'MagneticStoreRetentionPeriodInDays': CT_TTL_DAYS
}
try:
    write_client.update_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, RetentionProperties=retention_properties)
    print("Table updated")
except Exception as err:
    print("Update table failed:", err)



def submit_batch(records, counter):
        try:
            result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = TABLE_NAME, Records=records, CommonAttributes={})
            print("Processed [%d] records. WriteRecords Status: [%s]" % (counter, result['ResponseMetadata']['HTTPStatusCode']))
        except write_client.exceptions.RejectedRecordsException as err:
            print("RejectedRecords: ", err)
            for rr in err.response["RejectedRecords"]:
                print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            print("Other records were written successfully")
        except Exception as err:
            print("Error:", err)
            exit()

def write_data():

    # Write data

    print("Writing records")

    start_time = time.time()

    records = []
    counter = 0

    for d in Datas.iterrows():

        Time = d[1]['TimeStamp_spotinfo'].split('+')[0]
        if Time.split(' ')[0] == '2021-11-24':
            break
        Time = time.strptime(Time, '%Y-%m-%d %H:%M:%S')
        Time = time.mktime(Time)
        Time = str(int(Time * 1000))

        dimensions = [
            {'Name': 'InstanceType', 'Value': d[1]['InstanceType']},
            {'Name': 'Region', 'Value': d[1]['Region']},
            {'Name': 'AvailabilityZoneId', 'Value': d[1]['AvailabilityZoneId']}
        ]

        Score = {
            'Name': 'Score',
            'Value': str(d[1]['Score']),
            'Type': 'BIGINT',
        }

        Frequency = {
            'Name': 'Frequency',
            'Value': str(d[1]['Frequency']),
            'Type': 'DOUBLE',
        }

        Price = {
            'Name': 'Price',
            'Value': str(d[1]['Price']),
            'Type': 'DOUBLE',
        }

        Spot_data = {
            'Dimensions': dimensions,
            'MeasureName': 'Spot_data',
            'MeasureValueType': 'MULTI',
            'MeasureValues': [
                Score,
                Frequency,
                Price
            ],
            'Time': Time,
            'Version': 2
        }

        records.append(Spot_data)
        counter += 1

        if len(records) == 100:
            submit_batch(records, counter)
            records = []
        
            break

    if len(records) != 0:
        submit_batch(records, counter)

    end_time = time.time()

    duration = end_time - start_time
    print('\nDuration :', duration, '(s)')

write_data()