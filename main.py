import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

def list_to_dict(element, columns):
    """
    Recebe 2 listas
    Retorna 1 Dicionário.
    """
    return dict(zip(columns,element))

def text_to_list(element, delimitador='|'):
    """
    Recebe um texto com delimitador
    Retorna uma lista de elementos pelo delimitador.
    """
    return element.split(delimitador)

def do_date(data):
    """
    Recebe um dicionário e cria uma nova coluna com ANO-MES
    Retorna o mesmo dicionário com o novo campo
    """
    data['ano_mes'] = '-'.join(data['data_iniSE'].split('-')[:2])
    return data

def key_uf(element):
    
    chave = element['uf']
    return (chave, element)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >>
        ReadFromText('./data/casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(text_to_list)
    | "De Lista para Dicionário" >> beam.Map(list_to_dict, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(do_date)
    | "Criar chave pelo estado" >> beam.Map(key_uf)
    | "Agrupar pelo estado" >> beam.GroupBy()
    | "Mostar Resultados" >> beam.Map(print)
)

pipeline.run()