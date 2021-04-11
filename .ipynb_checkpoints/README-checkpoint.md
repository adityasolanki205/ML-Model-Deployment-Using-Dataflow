# ML model deployment using DataFlow
This is one of the part of **Introduction to Apache Beam using Python** Repository. Here we will try to learn basics of Apache Beam to create **Batch** pipelines and deploy a Machine learning model. We will learn step by step process on how to create a this pipeline using [German Credit Risk](https://www.kaggle.com/uciml/german-credit). The complete process is divided into 7 parts:

1. **Reading the data**
2. **Parsing the data**
3. **Performing Type Convertion**
4. **Predicting Customer segments**
5. **Inserting Data in Bigquery**


## Motivation
For the last two years, I have been part of a great learning curve wherein I have upskilled myself to move into a Machine Learning and Cloud Computing. This project was practice project for all the learnings I have had. This is first of the many more to come. 
 

## Libraries/frameworks used

<b>Built with</b>
- [Apache Beam](https://beam.apache.org/documentation/programming-guide/)
- [Anaconda](https://www.anaconda.com/)
- [Python](https://www.python.org/)
- [Google DataFlow](https://cloud.google.com/dataflow)
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Google Bigquery](https://cloud.google.com/bigquery)

## Cloning Repository

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/ML-Model-Deployment-Using-Dataflow.git
```

## Pipeline Construction

Below are the steps to setup the enviroment and run the codes:

1. **Setup**: First we will have to setup free google cloud account which can be done [here](https://cloud.google.com/free). Then we need to Download the data from [German Credit Risk](https://www.kaggle.com/uciml/german-credit).

2. **Cloning the Repository to Cloud SDK**: We will have to copy the repository on Cloud SDK using below command:

```bash
    # clone this repo:
    git clone https://github.com/adityasolanki205/ML-Model-Deployment-Using-Dataflow.git
```

3. **Reading the Data**: Now we will go step by step to create a pipeline starting with reading the data. The data is read using **beam.io.ReadFromText()**. Here we will just read the input values and save it in a file. The output is stored in text file named simpleoutput.

```python
    def run(argv=None, save_main_session=True):
        parser = argparse.ArgumentParser()
        parser.add_argument(
          '--input',
          dest='input',
          help='Input file to process')
        parser.add_argument(
          '--output',
          dest='output',
          default='../output/result.txt',
          help='Output file to write results to.')
        known_args, pipeline_args = parser.parse_known_args(argv)
        options = PipelineOptions(pipeline_args)
        with beam.Pipeline(options=PipelineOptions()) as p:
            data = (p 
                         | beam.io.ReadFromText(known_args.input)
                         | 'Writing output' >> beam.io.WriteToText(known_args.output)
                   ) 
    if __name__ == '__main__':
        run()
``` 
4. **Parsing the data**: After reading the input file we will split the data using split(). Data is segregated into different columns to be used in further steps. We will **ParDo()** to create a split function. The output of this step is present in split text file.

```python
    class Split(beam.DoFn):
        #This Function Splits the Dataset into a dictionary
        def process(self, element): 
            serial_number,
            Existing_account,
            Duration_month,
            Credit_history,
            Purpose,
            Credit_amount,
            Saving,
            Employment_duration,
            Installment_rate,
            Personal_status,
            Debtors,
            Residential_Duration,
            Property,
            Age,
            Installment_plans,
            Housing,
            Number_of_credits
            Job,
            Liable_People,
            Telephone,
            Foreign_worker,
            Classification = element.split(' ')
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
    def run(argv=None, save_main_session=True):
        ...
        with beam.Pipeline(options=PipelineOptions()) as p:
            data = (p 
                     | beam.io.ReadFromText(known_args.input) )
            parsed_data = (data 
                     | 'Parsing Data' >> beam.ParDo(Split())
                     | 'Writing output' >> beam.io.WriteToText(known_args.output))

    if __name__ == '__main__':
        run()
``` 
5. **Performing Type Convertion**: After Filtering we will convert the datatype of numeric columns from String to Int or Float datatype. Here we will use **Map()** to apply the Convert_Datatype(). The output of this step is saved in Converted_datatype text file.

```python
    ... 
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
    ...
    def run(argv=None, save_main_session=True):
        ...
        with beam.Pipeline(options=PipelineOptions()) as p:
            data = (p 
                     | beam.io.ReadFromText(known_args.input) )
            parsed_data = (data 
                     | 'Parsing Data' >> beam.ParDo(Split()))
            Converted_data = (parsed_data
                     | 'Convert Datatypes' >> beam.Map(Convert_Datatype)
                     | 'Writing output' >> beam.io.WriteToText(known_args.output))

    if __name__ == '__main__':
        run()
```

6. **Predicting Customer segments**: Now we will implement the machine learning model. If you wish to learn how this machine learning model was created, please visit this [repository](https://github.com/adityasolanki205/German-Credit). We will save this model using JobLib library. To load the sklearn model we will have to follow the steps mentioned below:
    - Download the Model from Google Storage bucket using download_blob method
    
    - Load the model using setup() method in Predict_data() class
    
    - Predict Customer segments in the input using Predict() method of sklearn
    
    - Add Prediction column in the input

```python
    ... 
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
    ...
    def run(argv=None, save_main_session=True):
        ...
        with beam.Pipeline(options=PipelineOptions()) as p:
            data           = (p 
                             | beam.io.ReadFromText(known_args.input, skip_header_lines=1) )
            Parsed_data    = (data 
                             | 'Parsing Data' >> beam.ParDo(Split()))
            Converted_data = (Parsed_data
                             | 'Convert Datatypes' >> beam.Map(Convert_Datatype))

            Prediction     = (Converted_data 
                             | 'Predition' >> beam.ParDo(Predict_Data(project=PROJECT_ID, 
                                                         bucket_name='gs://batch-pipeline-testing', 
                                                         model_path='Selected_Model.pkl',
                                                         destination_name='Selected_model.pkl')))
            Output         = (Prediction
                             | 'Saving the output' >> beam.io.WriteToText(known_args.output))
    if __name__ == '__main__':
        run()
```

7. **Inserting Data in Bigquery**: Final step in the Pipeline it to insert the data in Bigquery. To do this we will use **beam.io.WriteToBigQuery()** which requires Project id and a Schema of the target table to save the data. 

```python
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    import argparse
    
    SCHEMA = 
    '
        Existing_account:INTEGER,
        Duration_month:FLOAT,
        Credit_history:INTEGER,
        Purpose:INTEGER,
        Credit_amount:FLOAT,
        Saving:INTEGER,
        Employment_duration:INTEGER,
        Installment_rate:FLOAT,
        Personal_status:INTEGER,
        Debtors:INTEGER,
        Residential_Duration:FLOAT,
        Property:INTEGER,
        Age:FLOAT,
        Installment_plans:INTEGER,
        Housing:INTEGER,
        Number_of_credits:FLOAT,
        Job:INTEGER,
        Liable_People:FLOAT,
        Telephone:INTEGER,
        Foreign_worker:INTEGER,
        Prediction:INTEGER
    '
    ...
    def run(argv=None, save_main_session=True):
        ...
        parser.add_argument(
          '--project',
          dest='project',
          help='Project used for this Pipeline')
        ...
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
                                                                  bucket_name='gs://batch-pipeline-testing', 
                                                                  model_path='Selected_Model.pkl',
                                                                  destination_name='Selected_model.pkl')))
            output       = ( Prediction      
                         | 'Writing to bigquery' >> beam.io.WriteToBigQuery(
                               '{0}:GermanCredit.GermanCreditTable'.format(PROJECT_ID),
                               schema=SCHEMA,
                               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    if __name__ == '__main__':
        run()        
```

## Tests
To test the code we need to do the following:

    1. Copy the repository in Cloud SDK using below command:
    git clone https://github.com/adityasolanki205/ML-Model-Deployment-Using-Dataflow.git
    
    2. Create a Storage Bucket in asia-east1 by the name batch-pipeline-testing.
    
    3. Copy the data file in the cloud Bucket using the below commad
    cd ML-Model-Deployment-Using-Dataflow/data
    gsutil cp clean_customer_data.csv gs://batch-pipeline-testing/
    
    4. Create a Dataset in asia-east1 by the name GermanCredit
    
    5. Create a table in GermanCredit dataset by the name GermanCreditTable
    
    6. Install Apache Beam on the SDK using below command
    sudo pip3 install apache_beam[gcp]
    sudo pip3 install joblib
    sudo pip3 install sklearn
    
    7. Run the command and see the magic happen:
     python3 ml-pipeline.py \
     --runner DataFlowRunner \
     --project <Project Name> \
     --temp_location gs://batch-pipeline-testing/Batch/Temp \
     --staging_location gs://batch-pipeline-testing/Batch/Stage \
     --input gs://batch-pipeline-testing/clean_customer_data.csv \
     --region asia-east1 \
     --job_name ml-germananalysis \
     --save_main_session True \
     --setup_file ./setup.py


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Beam](https://beam.apache.org/documentation/programming-guide/#triggers)
3. [Building Data Processing Pipeline With Apache Beam, Dataflow & BigQuery](https://towardsdatascience.com/apache-beam-pipeline-for-cleaning-batch-data-using-cloud-dataflow-and-bigquery-f9272cd89eba)
4. [Model deployment with Apache Beam and Dataflow](https://towardsdatascience.com/model-deployment-with-apache-beam-and-dataflow-be1175c96d1f)
