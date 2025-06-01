#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
import argparse
import numpy as np
import joblib
from google.cloud import storage
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions

from sklearn.ensemble import RandomForestClassifier

SCHEMA='Existing_account:INTEGER,Duration_month:FLOAT,Credit_history:INTEGER,Purpose:INTEGER,Credit_amount:FLOAT,Saving:INTEGER,Employment_duration:INTEGER,Installment_rate:FLOAT,Personal_status:INTEGER,Debtors:INTEGER,Residential_Duration:FLOAT,Property:INTEGER,Age:FLOAT,Installment_plans:INTEGER,Housing:INTEGER,Number_of_credits:FLOAT,Job:INTEGER,Liable_People:FLOAT,Telephone:INTEGER,Foreign_worker:INTEGER,Prediction:INTEGER'


class Split(beam.DoFn):
    #This Function Splits the Dataset into a dictionary
    def process(self, element):
        serial_number,Existing_account,Duration_month,Credit_history,Purpose,Credit_amount,Saving,Employment_duration,Installment_rate,Personal_status,Debtors,Residential_Duration,Property,Age,Installment_plans,Housing,Number_of_credits,Job,Liable_People,Telephone,Foreign_worker = element.split(',')
        return [{
            'Existing_account': str(Existing_account),
            'Duration_month': str(Duration_month),
            'Credit_history': str(Credit_history),
            'Purpose': str(Purpose),
            'Credit_amount': str(Credit_amount),
            'Saving': str(Saving),
            'Employment_duration':str(Employment_duration),
            'Installment_rate': str(Installment_rate),
            'Personal_status': str(Personal_status),
            'Debtors': str(Debtors),
            'Residential_Duration': str(Residential_Duration),
            'Property': str(Property),
            'Age': str(Age),
            'Installment_plans':str(Installment_plans),
            'Housing': str(Housing),
            'Number_of_credits': str(Number_of_credits),
            'Job': str(Job),
            'Liable_People': str(Liable_People),
            'Telephone': str(Telephone),
            'Foreign_worker': str(Foreign_worker),
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

def download_blob(bucket_name=None, source_blob_name=None, project=None, destination_file_name=None):
    storage_client = storage.Client(project)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    
class Predict_Data(beam.DoFn):
    def __init__(self,project=None, bucket_name=None, model_path=None, destination_name=None):
        self._model = None
        self._project = project
        self._bucket_name = bucket_name
        self._model_path = model_path
        self._destination_name = destination_name
        
    def setup(self):
        """Download sklearn model from GCS"""
        download_blob(bucket_name=self._bucket_name, 
                      source_blob_name=self._model_path,
                      project=self._project, 
                      destination_file_name=self._destination_name)
        self._model = joblib.load(self._destination_name)
        
    def process(self, element):
        """Predicting using developed model"""
        input_dat = {k: element[k] for k in element.keys()}
        tmp = np.array(list(i for i in input_dat.values()))
        tmp = tmp.reshape(1, -1)
        element['Prediction'] = self._model.predict(tmp).item()
        return [element]

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
                     | 'Predition' >> beam.ParDo(Predict_Data(project=PROJECT_ID, 
                                                              bucket_name='batch-pipeline-testing', 
                                                              model_path='Selected_Model.pkl',
                                                              destination_name='Selected_Model.pkl')))
        output       = ( Prediction      
                     | 'Writing to bigquery' >> beam.io.WriteToBigQuery(
                           '{0}:GermanCredit.GermanCreditTable'.format(PROJECT_ID),
                           schema=SCHEMA,
                           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                       ))
        
if __name__ == '__main__':
    run()