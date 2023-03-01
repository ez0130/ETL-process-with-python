from datetime import datetime
import glob
import pandas as pd

list_csv = glob.glob('*.csv')
#list_csv:['source1.csv', 'source2.csv']
list_json = glob.glob('*.json')
#list_json:['source1.json', 'source2.json']

#extract CSV
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

#extract json
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

df = extract_from_json('source1.json')

def extract():
    #crate an empty data frame to hold extraced data
    extracted_data = pd.DataFrame(columns =['name', 'height', 'weight'])
    
    #process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile),
                                               ignore_index=True)
    
    #proces all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile),
                                               ignore_index=True)
    return extracted_data

#transform data
def transform(data):
    #convert units
    data['height'] = round(data.height * 0.025)
    data['weight'] = round(data.weight *-.453)
    return data


#Load
def load(targetfille, data_to_load):
    data_to_load.to_csv(targetfile)
    
    targetfile = "transformed_data.csv"
    load(targetfile, transformed_data)
    

#logging entries
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(Timestamp_format)
    with open( 'logfile.txt', 'a') as f:
        f.write (timestamp + ',' + message + '\n')
        
        
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform Job Started")
transformed_data = transform(extracted_data)
log("Transform Job Ended")

log("Load Job Started")
load(targetfile, transformed_data)
log("Load Job Ended")

log ("ETL Job Ended")
