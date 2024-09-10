import pandas as pd
import connect as ct
import data  # Importar o arquivo que contém a função converte_uppercase

# Este documento faz a tratativa da tabela de LEADS do banco de dados

# Carregar o arquivo CSV
dadosLeads = pd.read_csv('df/leads/rd-supranet-leads-leads-ativos.csv', sep=",", index_col=False)

# Remover a coluna 'Unnamed: 0', se ela existir
dadosLeads = dadosLeads.drop(columns=['Unnamed: 0'], errors='ignore')

# Renomear a coluna "Data da primeira conversão" para "Data_Primeira_Conversao"
dadosLeads.rename(columns={'Data da primeira conversão': 'Data_Primeira_Conversao'}, inplace=True)

# Remover completamente o índice antes de exportar para SQL
dadosLeads = dadosLeads.reset_index(drop=True)

# Substituir os valores NaN por None, que o MySQL interpreta como NULL
df = dadosLeads.where(pd.notnull(dadosLeads), None)

# Aplicar a função para converter todo o DataFrame em maiúsculas
df = data.converte_uppercase(df)

# Converter a coluna de data "Data_Primeira_Conversao" para o formato datetime
df['Data_Primeira_Conversao'] = pd.to_datetime(df['Data_Primeira_Conversao'], errors='coerce')

# Definir a consulta SQL para inserir os valores
sql_insert = """
INSERT INTO leads (
    Email, Nome, Telefone, Celular, Data_Primeira_Conversao
) VALUES (%s, %s, %s, %s, %s)
"""

# Iterar sobre o DataFrame e inserir os valores no banco de dados
for index, row in df.iterrows():
    # Certificar que o campo Email não é nulo
    assert pd.notnull(row['Email']), f"Erro: O campo Email está nulo na linha {index}"
    
    # Substituir valores nulos (NaN) por None, que o MySQL reconhece como NULL
    valores = (
        row['Email'],  # Verificado que o Email não é nulo
        row['Nome'] if pd.notnull(row['Nome']) else None,
        row['Telefone'] if pd.notnull(row['Telefone']) else None,
        row['Celular'] if pd.notnull(row['Celular']) else None,
        row['Data_Primeira_Conversao'] if pd.notnull(row['Data_Primeira_Conversao']) else None
    )
    
    # Executar a inserção no banco de dados
    ct.executa(sql_insert, valores)
