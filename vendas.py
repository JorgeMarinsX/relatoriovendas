import data
import pandas as pd
import connect as ct
import queries
from sqlalchemy import Table, MetaData

# Este documento faz a tratativa da tabela de VENDAS do banco de dados.

# Carregar o arquivo CSV e ajustar os nomes das colunas
dadosVendas = pd.read_csv('df/vendas/vendas_novembro_em_diante.csv', sep=",", index_col=False)
dadosVendas.columns = dadosVendas.columns.str.replace(' ', '_')
print("Colunas disponíveis no DataFrame após ajuste:", dadosVendas.columns)

# Remover a coluna 'Unnamed: 0', se ela existir
dadosVendas = dadosVendas.drop(columns=['Unnamed: 0'], errors='ignore')

# Aplicar a normalização dos dados
dadosTratados = data.normalizaDados(data.limpaValoresVendas(dadosVendas))

# Remover completamente o índice antes de exportar para SQL
dadosTratados = dadosTratados.reset_index(drop=True)

# Substituir os valores NaN por None, que o MySQL interpreta como NULL
df = dadosTratados.where(pd.notnull(dadosTratados), None)

# Verificar se ainda há valores NaN ou None nas colunas
print("Verificando valores nulos no DataFrame antes da inserção:", df.isnull().sum())

# Converter as colunas de datas, se necessário
df['Data_Adesao'] = pd.to_datetime(df['Data_Adesao'], errors='coerce')
df['Data_Cancelamento'] = pd.to_datetime(df['Data_Cancelamento'], errors='coerce')
df['Proximo_Reajuste'] = pd.to_datetime(df['Proximo_Reajuste'], errors='coerce')

# Ler os dados existentes da tabela vendas no banco de dados
df_existente = queries.verificaCodContrato()

# Remover duplicatas com base no campo 'Cod_Contrato'
df_novos_dados = df[~df['Cod_Contrato'].isin(df_existente['Cod_Contrato'])]

# Inserir novos dados no banco de dados
if not df_novos_dados.empty:
    # Conectar ao banco de dados usando SQLAlchemy
    engine = ct.conecta()
    metadata = MetaData()
    
    # Refletir a tabela 'vendas' no banco de dados
    vendas = Table('vendas', metadata, autoload_with=engine)

    # Preparar a inserção dos valores
    valores_para_inserir = df_novos_dados.to_dict(orient='records')

    # Utilizar a função executa para inserção em massa
    ct.executa(instrucao=None, valores=valores_para_inserir, tabela=vendas, bulk_insert=True)

    print(f"{len(df_novos_dados)} novos registros inseridos com sucesso.")

else:
    print("Nenhum novo dado para inserir.")