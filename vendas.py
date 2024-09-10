import data
import pandas as pd
import connect as ct

# Este documento faz a tratativa da tabela de VENDAS do banco de dados.

# Carregar o arquivo CSV e garantir que o índice não seja carregado como coluna
dadosVendas = pd.read_csv('df/vendas/vendas_total.csv', sep=",", index_col=False)

# Verificar os nomes das colunas para garantir que estão corretos
print("Colunas disponíveis no DataFrame:", dadosVendas.columns)

# Remover a coluna 'Unnamed: 0', se ela existir
dadosVendas = dadosVendas.drop(columns=['Unnamed: 0'], errors='ignore')

# Aplicar a normalização dos dados, agora corrigido para usar 'Cod Cliente' corretamente
dadosTratados = data.normalizaDados(data.limpaValoresVendas(dadosVendas))

# Remover completamente o índice antes de exportar para SQL
dadosTratados = dadosTratados.reset_index(drop=True)

# Substituir os valores NaN por None, que o MySQL interpreta como NULL
df = dadosTratados.where(pd.notnull(dadosTratados), None)

# Verificar se ainda há valores NaN ou None nas colunas
print("Verificando valores nulos no DataFrame antes da inserção:", df.isnull().sum())

# Converter as colunas de datas, se necessário
df['Data Adesão'] = pd.to_datetime(df['Data Adesão'], errors='coerce')
df['Data Cancelamento'] = pd.to_datetime(df['Data Cancelamento'], errors='coerce')
df['Próximo Reajuste'] = pd.to_datetime(df['Próximo Reajuste'], errors='coerce')

# Definir a consulta SQL para inserir os valores
sql_insert = """
INSERT INTO vendas (
    Cod_Contrato, Valor_Contrato, Cod_Cliente, Cliente, Tipo_Pessoa, Celular, Telefone, Email,
    Vigencia, Cidade, Bairro, Proximo_Reajuste, Ponto_Acesso, Plano_Acesso, Valor_Plano, Vendedor,
    Data_Adesao, Data_Cancelamento, Status_Contrato, Motivo_Cancelamento
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Iterar sobre o DataFrame e inserir os valores no banco de dados
for index, row in df.iterrows():
    # Verificar se o campo 'Email' não é nulo ou vazio
    if pd.isna(row['Email']) or row['Email'].strip() == '':
        print(f"Linha {index} rejeitada: Email vazio ou nulo.")
        continue  # Ignorar esta linha e passar para a próxima
    
    valores = (
        row['Cod Contrato'], row['Valor Contrato'], row['Cod Cliente'], row['Cliente'], row['Tipo Pessoa'], 
        row['Celular'], row['Telefone'], row['Email'], row['Vigência'], row['Cidade'], row['Bairro'], 
        row['Próximo Reajuste'], row['Ponto Acesso'], row['Plano Acesso'], row['Valor Plano'], row['Vendedor'], 
        row['Data Adesão'], row['Data Cancelamento'], row['Status Contrato'], row['Motivo Cancelamento']
    )
    
    # Executar a inserção no banco de dados
    ct.executa(sql_insert, valores)
