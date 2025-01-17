import pandas as pd

# Caminho do arquivo de entrada e saída
input_file = "C:\\Fabio\\DATAFLOW\\entrada\\browser_scc_total_fl.csv"
output_file = "C:\\Fabio\DATAFLOW\\saida\\bronze_profissionais.csv"

# Ler o arquivo CSV, especificando o delimitador e o tipo de codificação (ajuste se necessário)
df = pd.read_csv(input_file, sep=',', encoding='utf-8')

# Selecionar as colunas desejadas
df_selecionado = df[['data_processamento', 'identificador_profissional', 'nome_profissional']]

# Salvar o DataFrame em um novo arquivo CSV
df_selecionado.to_csv(output_file, index=False)

print("Arquivo salvo com sucesso!")

# subida
