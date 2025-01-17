import apache_beam as beam
import os

#  configurar as credenciais do Google Cloud para que o seu 
#  script possa acessar o Google Cloud Storage e o Dataflow.

serviceAccount = r'C:\Fabio\DATAFLOW\entrada\serviceAccount.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

pl = beam.Pipeline()

class filtro(beam.DoFn):
    def process(self,record):
        if int(record[8]) > 0:
            return [record]
 
Tempo_Atraso = (
    pl
    | "Importar Dados Atraso" >> beam.io.ReadFromText(r'C:\Fabio\DATAFLOW\entrada\browser_scc_total_fl.csv')
    | "Separar por virgulas Atraso" >> beam.map(lambda record: record.split(','))
    | "Pegar voos com atraso" >> beam.ParDo(filtro())
    | "Criar por atraso" >> beam.map(lambda record: (record[4] , int(record[8])))
    | "Somar por key" >> beam.CombinePerKey(sum)
)

        