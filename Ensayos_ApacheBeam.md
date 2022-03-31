# CARTO
#Lectura de ficheros txt de un bucket de GCS, realizamos varias transformaciones para contar el numero de palabras que tiene el fichero y lo subimos a otra carpeta del bucket
 
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
 
bucket_name='jropero'

#Funcion ParDo
class prueba(beam.DoFn):
  def process(self,element):
    client= storage.Client()
    bucket = client.get_bucket(element)
    blobs = bucket.list_blobs()
    names=[]
    for blob in blobs:
      name=blob.name
      names.append(name)
    files=[]
    for name in names:
      files.append(name)

    for i in files:
      blob = bucket.get_blob(i)
      df= blob.download_as_string()
      df= df.upper()
      res = len(df.split())
      escrito = ("El documento " + blob.name + " tiene "+ str(res) + " palabras" )
      bucket = client.get_bucket('jropero')
      blobe = bucket.blob("salida/" + blob.name+" count")
      yield blobe.upload_from_string(escrito)    yield blobe.upload_from_string(escrito)
     
#Para prueba en Local:

p= beam.Pipeline(InteractiveRunner())
input= (p | "Creamoss la PCollection" >> beam.Create([bucket_name])
  | "Transformaciones" >>beam.ParDo(prueba()))
 
ib.show(input)

#Para Dataflow:

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='ensayocarto-345108',
    job_name='pruebadataflow',
    temp_location='gs://jropero/temp',
    region ='europe-west1',
    numWorkers = 2
)


with beam.Pipeline(options=beam_options) as p:
  input= (p | "Creamoss la PCollection" >> beam.Create([files])
    | "Transformaciones" >>beam.ParDo(prueba()))

 
    
