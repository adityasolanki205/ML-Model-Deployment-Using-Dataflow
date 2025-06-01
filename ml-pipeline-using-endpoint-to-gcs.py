#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
import argparse
import numpy as np
import joblib
from google.cloud import storage
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from google.cloud import aiplatform


from sklearn.ensemble import RandomForestClassifier

#SCHEMA='Existing_account:INTEGER,Duration_month:FLOAT,Credit_history:INTEGER,Purpose:INTEGER,Credit_amount:FLOAT,Saving:INTEGER,Employment_duration:INTEGER,Installment_rate:FLOAT,Personal_status:INTEGER,Debtors:INTEGER,Residential_Duration:FLOAT,Property:INTEGER,Age:FLOAT,Installment_plans:INTEGER,Housing:INTEGER,Number_of_credits:FLOAT,Job:INTEGER,Liable_People:FLOAT,Telephone:INTEGER,Foreign_worker:INTEGER,Prediction:INTEGER'
SCHEMA = {
  'fields': [
    {'name': 'Existing_account', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Duration_month', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Credit_history', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Purpose', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Credit_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Saving', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Employment_duration', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Installment_rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Personal_status', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Debtors', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Residential_Duration', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Property', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Age', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Installment_plans', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Housing', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Number_of_credits', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Job', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Liable_People', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'Telephone', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Foreign_worker', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Prediction', 'type': 'INTEGER', 'mode': 'NULLABLE'}
  ]
}

class Split(beam.DoFn):
    #This Function Splits the Dataset into a dictionary
    def process(self, element):
        serial_number,Existing_account,Duration_month,Credit_history,Purpose,Credit_amount,Saving,Employment_duration,Installment_rate,Personal_status,Debtors,Residential_Duration,Property,Age,Installment_plans,Housing,Number_of_credits,Job,Liable_People,Telephone,Foreign_worker = element.split(',')
        return [{
            'Existing_account': int(Existing_account),
            'Duration_month': float(Duration_month),
            'Credit_history': int(Credit_history),
            'Purpose': int(Purpose),
            'Credit_amount': float(Credit_amount),
            'Saving': int(Saving),
            'Employment_duration':int(Employment_duration),
            'Installment_rate': float(Installment_rate),
            'Personal_status': int(Personal_status),
            'Debtors': int(Debtors),
            'Residential_Duration': float(Residential_Duration),
            'Property': int(Property),
            'Age': float(Age),
            'Installment_plans':int(Installment_plans),
            'Housing': int(Housing),
            'Number_of_credits': float(Number_of_credits),
            'Job': int(Job),
            'Liable_People': float(Liable_People),
            'Telephone': int(Telephone),
            'Foreign_worker': int(Foreign_worker),
        }]

def Convert_Datatype(data):
    #This will convert the datatype of columns from String to integers or Float values
    data['Duration_month'] = float(data['Duration_month']) if 'Duration_month' in data else None
    data['Credit_amount'] = float(data['Credit_amount']) if 'Credit_amount' in data else None
    data['Installment_rate'] = float(data['Installment_rate']) if 'Installment_rate' in data else None
    data['Residential_Duration'] = float(data['Residential_Duration']) if 'Residential_Duration' in data else None
    data['Age'] = float(data['Age']) if 'Age' in data else None
    data['Number_of_credits'] = float(data['Number_of_credits']) if 'Number_of_credits' in data else None
    data['Liable_People'] = float(data['Liable_People']) if 'Liable_People' in data else None
    data['Existing_account'] =  int(data['Existing_account']) if 'Existing_account' in data else None
    data['Credit_history'] =  int(data['Credit_history']) if 'Credit_history' in data else None
    data['Purpose'] =  int(data['Purpose']) if 'Purpose' in data else None
    data['Saving'] =  int(data['Saving']) if 'Saving' in data else None
    data['Employment_duration'] =  int(data['Employment_duration']) if 'Employment_duration' in data else None
    data['Personal_status'] =  int(data['Personal_status']) if 'Personal_status' in data else None
    data['Debtors'] =  int(data['Debtors']) if 'Debtors' in data else None
    data['Property'] =  int(data['Property']) if 'Property' in data else None
    data['Installment_plans'] =  int(data['Installment_plans']) if 'Installment_plans' in data else None
    data['Housing'] =  int(data['Housing']) if 'Housing' in data else None
    data['Job'] =  int(data['Job']) if 'Job' in data else None
    data['Telephone'] =  int(data['Telephone']) if 'Telephone' in data else None
    data['Foreign_worker'] =  int(data['Foreign_worker']) if 'Foreign_worker' in data else None
    return data

def call_vertex_ai(data, project_id='827249641444'):
    aiplatform.init(project='827249641444', location='asia-south1')
    feature_order = ['Existing_account', 'Duration_month', 'Credit_history', 'Purpose',
                 'Credit_amount', 'Saving', 'Employment_duration', 'Installment_rate',
                 'Personal_status', 'Debtors', 'Residential_Duration', 'Property', 'Age',
                 'Installment_plans', 'Housing', 'Number_of_credits', 'Job', 
                 'Liable_People', 'Telephone', 'Foreign_worker']
    # client = aiplatform.PredictionServiceClient()
    # endpoint = client.endpoint_path(project='827249641444', location='asia-south1', endpoint='6402372645655937024')
    endpoint = aiplatform.Endpoint(endpoint_name=f"projects/827249641444/locations/asia-south1/endpoints/6402372645655937024")
    features = [data[feature] for feature in feature_order]
    response = endpoint.predict(
        instances=[features]
    )
    
    prediction = response.predictions[0]
    data['Prediction'] = prediction
    return data

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      help='Input file to process')
    parser.add_argument(
      '--project',
      dest='project',
      help='Project used for this Pipeline')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    PROJECT_ID = known_args.project
    with beam.Pipeline(options=PipelineOptions()) as p:
        data         = (p 
                     | beam.io.ReadFromText(known_args.input, skip_header_lines=1) )
        parsed_data  = (data 
                     | 'Parsing Data' >> beam.ParDo(Split()))
        Converted_data = (parsed_data
                     | 'Convert Datatypes' >> beam.Map(Convert_Datatype))
        Prediction   = (Converted_data
                    |'Get Inference' >> beam.Map(call_vertex_ai, project_id='827249641444'))
        output = ( Prediction
                   | 'Write to GCS' >> beam.io.WriteToText('gs://test_german_data/output/result.csv'))
        
if __name__ == '__main__':
    run()