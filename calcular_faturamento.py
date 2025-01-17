import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None):
    options = PipelineOptions(argv)

    # Opções de entrada e saída
    input_file = 'gs://srvdor-01/Cassiopeia/clientes.txt'
    output_file = 'gs://srvdor-01/Cassiopeia/saida/faturamento_mes.txt'

    # Ler o arquivo CSV e definir o esquema
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Ler arquivo' >> beam.io.ReadFromText(input_file, skip_header_rows=1)
            | 'Parsear CSV' >> beam.Map(lambda x: x.split(','))
            | 'Calcular faturamento' >> beam.Map(lambda record: (record[0], float(record[1]) * float(record[2])))
            | 'Escrever resultado' >> beam.io.WriteToText(output_file, file_name_suffix='.txt')
        )

if __name__ == '__main__':
  run()