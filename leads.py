import pandas as pd
import connect as ct
import data
import queries

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

# Ler os dados existentes da tabela leads no banco de dados
df_existente = queries.verificaLeadsPorEmail()

# Verificar se há dados existentes no banco
if df_existente.empty:
    print("Nenhum dado existente encontrado no banco. Prosseguindo apenas com inserções.")
    df_novos_dados = df
    df_atualizar = pd.DataFrame()  # Não há registros para atualizar se o banco estiver vazio
else:
    # Função para verificar diferenças
    def verificar_diferencas(df_novo, df_existente):
        # Identificar as diferenças, adicionando os sufixos _novo e _existente
        df_atualizar = pd.merge(df_novo, df_existente, on='Email', how='inner', suffixes=('_novo', '_existente'))
        
        # Verificar se algum valor mudou
        colunas_para_comparar = ['Nome', 'Telefone', 'Celular', 'Data_Primeira_Conversao']
        
        # Manter apenas as linhas onde alguma coluna mudou
        for col in colunas_para_comparar:
            df_atualizar = df_atualizar[df_atualizar[col + '_novo'] != df_atualizar[col + '_existente']]
        
        # Retornar apenas as colunas com o sufixo _novo para atualização no banco
        return df_atualizar[[col + '_novo' for col in colunas_para_comparar] + ['Email']]

    # Remover duplicatas com base no campo 'Email'
    df_novos_dados = df[~df['Email'].isin(df_existente['Email'])]
    df_atualizar = verificar_diferencas(df, df_existente)  # Dados a atualizar

# Verificar se há novos dados para inserir
if not df_novos_dados.empty:
    sql_insert = """
    INSERT INTO leads (
        Email, Nome, Telefone, Celular, Data_Primeira_Conversao
    ) VALUES (%s, %s, %s, %s, %s)
    """

    # Iterar sobre os novos dados e inserir no banco de dados
    for index, row in df_novos_dados.iterrows():
        if row['Email'] is None:
            print(f"Linha {index} rejeitada: Email vazio ou nulo.")
            continue  # Ignorar esta linha e passar para a próxima
        
        valores = (
            row['Email'],
            row['Nome'],
            row['Telefone'],
            row['Celular'],
            row['Data_Primeira_Conversao']
        )
        
        # Executar a inserção no banco de dados
        ct.executa(sql_insert, valores)
    
    print(f"{len(df_novos_dados)} novos registros inseridos com sucesso.")
else:
    print("Nenhum novo dado para inserir.")

# Atualizar registros com valores diferentes
if not df_atualizar.empty:
    sql_update = """
    UPDATE leads SET 
        Nome = %s, Telefone = %s, Celular = %s, Data_Primeira_Conversao = %s 
        WHERE Email = %s
    """
    
    for index, row in df_atualizar.iterrows():
        valores = (
            row['Nome_novo'], row['Telefone_novo'], row['Celular_novo'], row['Data_Primeira_Conversao_novo'], row['Email']
        )
        
        # Executar a atualização no banco de dados
        ct.executa(sql_update, valores)
    
    print(f"{len(df_atualizar)} registros atualizados com sucesso.")
else:
    print("Nenhum registro para atualizar.")