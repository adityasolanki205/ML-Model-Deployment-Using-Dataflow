#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import numpy as np
import joblib

SCHEMA='Duration_month:INTEGER,Credit_history:STRING,Credit_amount:FLOAT,Saving:STRING,Employment_duration:STRING,Installment_rate:INTEGER,Personal_status:STRING,Debtors:STRING,Residential_Duration:INTEGER,Property:STRING,Age:INTEGER,Installment_plans:STRING,Housing:STRING,Number_of_credits:INTEGER,Job:STRING,Liable_People:INTEGER,Telephone:STRING,Foreign_worker:STRING,Classification:INTEGER,Month:STRING,days:INTEGER,File_Month:STRING,Version:INTEGER'


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

class Predict_Data(beam.DoFn):
    def __init__(self, model_path=None):
        self._model = None
        self._model_path = model_path
        
    def setup(self):
        """Download sklearn model from GCS"""
        self._model = joblib.load(self._model_path)
        
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
    parser.add_argument(
      '--output',
      dest='output',
      default='../output/result.txt',
      help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    PROJECT_ID = known_args.project
    with beam.Pipeline(options=PipelineOptions()) as p:
        data = (p 
                     | beam.io.ReadFromText(known_args.input, skip_header_lines=1) )
        
        Parsed_data  = (data 
                      | 'Parsing Data' >> beam.ParDo(Split()))
        
        Converted_data = (Parsed_data
                     | 'Convert Datatypes' >> beam.Map(Convert_Datatype))
        
        Prediction   = (Converted_data 
                     | 'Predicting the Segement' >> beam.ParDo(Predict_Data(model_path='Selected_model.pkl'
                                                              )))
        Output = (Prediction
                     | 'Saving the output' >> beam.io.WriteToText(known_args.output))
        
if __name__ == '__main__':
    run()
