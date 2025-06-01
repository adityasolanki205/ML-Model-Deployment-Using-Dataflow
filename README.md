# ML model deployment using DataFlow
This is one of the **Introduction to Apache Beam using Python** Repositories. Here we will try to learn basics of Apache Beam to create **Batch** pipelines and deploy a Machine learning model. We will learn step by step process on how to create a this pipeline using [German Credit Risk](https://www.kaggle.com/uciml/german-credit). The complete process is divided into 7 parts:

1. **Reading the data**
2. **Parsing the data**
3. **Performing Type Convertion**
4. **Creating Vertex AI Endpoint**
5. **Predicting Customer segments by downloading model everytime**
6. **Predicting Customer segments using Vertex AI Endpoint**
7. **Inserting Data in Bigquery**


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
        return return [{
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
6. **Creating Vertex AI endpoint**: Now we will create an endpoint in vertex AI. To do that we will have to follow below steps.
    - Save the Model to GCS Bucket: To do that we will move our model to a GCS bucket
  
      ```python
            from google.cloud import storage
            storage_client = storage.Client()
            bucket = storage_client.bucket("test_german_data")
            model_artifact = bucket.blob('model-artifact/'+'model.joblib')
            model_artifact.upload_from_filename('model.joblib')
      ```
    
    - Register the model to Vertex AI Model Regsitry: To Upload to Model registry we will use the below code
      ```python
            from google.cloud.resourcemanager_v3 import FoldersAsyncClient
            from google.cloud import aiplatform
            from google.cloud.aiplatform.explain import ExplanationSpec
            display_name = "german_credit-model-sdk"
            artifact_uri = "gs://test_german_data/model-artifact"
            serving_container_image_uri = "asia-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-5:latest"
            
            model = aiplatform.Model.upload(
                    display_name=display_name,
                    artifact_uri=artifact_uri,
                    location='asia-south1',
                    serving_container_image_uri=serving_container_image_uri,
                    sync=False
                )
      ```
    - Create an online prediction endpoint: To Create the Endpoint we will use the code below
  
      ```python
            from google.cloud.resourcemanager_v3 import FoldersAsyncClient
            from google.cloud import aiplatform
            from google.cloud.aiplatform.explain import ExplanationSpe
            deployed_model_display_name = "german-credit-model-endpoint"
            traffic_split = {"0": 100}
            machine_type = "n1-standard-4"
            min_replica_count = 1
            max_replica_count = 1
            
            endpoint = model.deploy(
                    deployed_model_display_name=deployed_model_display_name,
                    machine_type=machine_type,
                    traffic_split = traffic_split,
                    min_replica_count=min_replica_count,
                    max_replica_count=max_replica_count
            )
      ```

 
7. **Predicting Customer segments by downloading model everytime**: Now we will implement the machine learning model. If you wish to learn how this machine learning model was created, please visit this [repository](https://github.com/adityasolanki205/German-Credit). We will save this model using JobLib library. To load the sklearn model we will have to follow the steps mentioned below:
    - Download the Model from Google Storage bucket using download_blob method
    
    - Load the model using setup() method in Predict_data() class
    
    - Predict Customer segments from the input data using Predict() method of sklearn
    
    - Add Prediction column in the output

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
8. **Predicting Customer segments using Vertex AI endpoint**: Now we will implement the machine learning model using the endpoint wee created above.
```python
    ... 
    def call_vertex_ai(data, project_id='827249641444'):
        aiplatform.init(project='827249641444', location='asia-south1')
        feature_order = ['Existing_account', 'Duration_month', 'Credit_history', 'Purpose',
                     'Credit_amount', 'Saving', 'Employment_duration', 'Installment_rate',
                     'Personal_status', 'Debtors', 'Residential_Duration', 'Property', 'Age',
                     'Installment_plans', 'Housing', 'Number_of_credits', 'Job', 
                     'Liable_People', 'Telephone', 'Foreign_worker']
        endpoint = aiplatform.Endpoint(endpoint_name=f"projects/827249641444/locations/asia-south1/endpoints/6402372645655937024")
        features = [data[feature] for feature in feature_order]
        response = endpoint.predict(
            instances=[features]
        )
        
        prediction = response.predictions[0]
        data['Prediction'] = int(prediction)
    return data
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
            Prediction   = (Converted_data
                    |'Get Inference' >> beam.Map(call_vertex_ai, project_id='827249641444'))
            output = ( Prediction
                        | 'Write to GCS' >> beam.io.WriteToText('gs://test_german_data/output/result.csv'))
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
                    |'Get Inference' >> beam.Map(call_vertex_ai, project_id='827249641444'))
            # Prediction     = (Converted_data 
            #                  | 'Predition' >> beam.ParDo(Predict_Data(project=PROJECT_ID, 
            #                                              bucket_name='gs://batch-pipeline-testing', 
            #                                              model_path='Selected_Model.pkl',
            #                                              destination_name='Selected_model.pkl')))
            output       = ( Prediction      
                        | 'Writing to bigquery' >> beam.io.WriteToBigQuery(
                            table='solar-dialect-264808:GermanCredit.GermanCreditTable',
                            schema=SCHEMA,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                       ))

    if __name__ == '__main__':
        run()        
```

## Tests
To test the code we need to do the following:

    1. Copy the repository in Cloud SDK using below command:
    git clone https://github.com/adityasolanki205/ML-Model-Deployment-Using-Dataflow.git
    
    2. Create a Storage Bucket in asia-south1 by the name test_german_data.
    
    3. Copy the machine learning model in the bucket
    cd ML-Model-Deployment-Using-Dataflow
    gsutil cp Selected_Model.pkl gs://batch-pipeline-testing/
    
    4. Copy the data file in the cloud Bucket using the below commad
    cd ML-Model-Deployment-Using-Dataflow/data
    gsutil cp clean_customer_data.csv gs://batch-pipeline-testing/
    
    5. Create a Dataset in asia-south1 by the name GermanCredit
    
    6. Create a table in GermanCredit dataset by the name GermanCreditTable and SCHEMA as mentioned in the code
    
    7. Install Apache Beam on the SDK using below command
    sudo pip3 install apache_beam[gcp]
    sudo pip3 install joblib
    sudo pip3 install sklearn
    
    8. Run the command and see the magic happen:
     python3 ml-streaming-pipeline.py \
            --runner DataFlowRunner \
            --project solar-dialect-264808 \
            --bucket_name test_german_data \
            --temp_location gs://test_german_data/Batch/Temp \
            --staging_location gs://test_german_data/Batch/Stage \
            --region asia-south1 \
            --job_name ml-stream-analysis \
            --input_subscription projects/solar-dialect-264808/subscriptions/german_credit_data-sub \
            --input_topic projects/solar-dialect-264808/topics/german_credit_data \
            --save_main_session True \
            --setup_file ./setup.py \
            --minNumWorkers 1 \
            --maxNumWorkers 4 \
            --streaming


## Credits
1. Akash Nimare's [README.md](https://gist.github.com/akashnimare/7b065c12d9750578de8e705fb4771d2f#file-readme-md)
2. [Apache Beam](https://beam.apache.org/documentation/programming-guide/#triggers)
3. [Building Data Processing Pipeline With Apache Beam, Dataflow & BigQuery](https://towardsdatascience.com/apache-beam-pipeline-for-cleaning-batch-data-using-cloud-dataflow-and-bigquery-f9272cd89eba)
4. [Model deployment with Apache Beam and Dataflow](https://towardsdatascience.com/model-deployment-with-apache-beam-and-dataflow-be1175c96d1f)
