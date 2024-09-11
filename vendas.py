import data
import pandas as pd
import connect as ct
import queries

# Este documento faz a tratativa da tabela de VENDAS do banco de dados.

# Carregar o arquivo CSV e ajustar os nomes das colunas
dadosVendas = pd.read_csv('df/vendas/vendas_total.csv', sep=",", index_col=False)
dadosVendas.columns = dadosVendas.columns.str.replace(' ', '_')
print("Colunas disponíveis no DataFrame após ajuste:", dadosVendas.columns)

# Remover a coluna 'Unnamed: 0', se ela existir
dadosVendas = dadosVendas.drop(columns=['Unnamed:_0'], errors='ignore')

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

# Verificar se há dados existentes no banco
if df_existente.empty:
    print("Nenhum dado existente encontrado no banco. Prosseguindo apenas com inserções.")
    df_novos_dados = df
    df_atualizar = pd.DataFrame()  # Não há registros para atualizar se o banco estiver vazio
else:
    # Função para verificar diferenças
    def verificar_diferencas(df_novo, df_existente):
        # Identificar as diferenças, adicionando os sufixos _novo e _existente
        df_atualizar = pd.merge(df_novo, df_existente, on='Cod_Contrato', how='inner', suffixes=('_novo', '_existente'))
        
        # Verificar se algum valor mudou
        colunas_para_comparar = ['Valor_Contrato', 'Cod_Cliente', 'Cliente', 'Tipo_Pessoa', 'Celular', 'Telefone', 
                                 'Email', 'Vigencia', 'Cidade', 'Bairro', 'Proximo_Reajuste', 'Ponto_Acesso', 
                                 'Plano_Acesso', 'Valor_Plano', 'Vendedor', 'Data_Adesao', 'Data_Cancelamento', 
                                 'Status_Contrato', 'Motivo_Cancelamento']
        
        # Manter apenas as linhas onde alguma coluna mudou
        for col in colunas_para_comparar:
            df_atualizar = df_atualizar[df_atualizar[col + '_novo'] != df_atualizar[col + '_existente']]
        
        # Retornar apenas as colunas com o sufixo _novo para atualização no banco
        return df_atualizar[[col + '_novo' for col in colunas_para_comparar] + ['Cod_Contrato']]

    # Remover duplicatas com base no campo 'Cod_Contrato'
    df_novos_dados = df[~df['Cod_Contrato'].isin(df_existente['Cod_Contrato'])]
    df_atualizar = verificar_diferencas(df, df_existente)  # Dados a atualizar

# Verificar se há novos dados para inserir
if not df_novos_dados.empty:
    sql_insert = """
    INSERT INTO vendas (
        Cod_Contrato, Valor_Contrato, Cod_Cliente, Cliente, Tipo_Pessoa, Celular, Telefone, Email,
        Vigencia, Cidade, Bairro, Proximo_Reajuste, Ponto_Acesso, Plano_Acesso, Valor_Plano, Vendedor,
        Data_Adesao, Data_Cancelamento, Status_Contrato, Motivo_Cancelamento
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for index, row in df_novos_dados.iterrows():
        # Verificar se o campo 'Email' não é nulo ou vazio
        if pd.isna(row['Email']) or row['Email'].strip() == '':
            print(f"Linha {index} rejeitada: Email vazio ou nulo.")
            continue 
        
        valores = (
            row['Cod_Contrato'], row['Valor_Contrato'], row['Cod_Cliente'], row['Cliente'], row['Tipo_Pessoa'], 
            row['Celular'], row['Telefone'], row['Email'], row['Vigencia'], row['Cidade'], row['Bairro'], 
            row['Proximo_Reajuste'], row['Ponto_Acesso'], row['Plano_Acesso'], row['Valor_Plano'], row['Vendedor'], 
            row['Data_Adesao'], row['Data_Cancelamento'], row['Status_Contrato'], row['Motivo_Cancelamento']
        )
        
        # Executar a inserção no banco de dados
        ct.executa(sql_insert, valores)
    
    print(f"{len(df_novos_dados)} novos registros inseridos com sucesso.")
else:
    print("Nenhum novo dado para inserir.")

# Atualizar registros com valores diferentes
if not df_atualizar.empty:
    sql_update = """
    UPDATE vendas SET 
        Valor_Contrato = %s, Cod_Cliente = %s, Cliente = %s, Tipo_Pessoa = %s, Celular = %s, Telefone = %s, Email = %s,
        Vigencia = %s, Cidade = %s, Bairro = %s, Proximo_Reajuste = %s, Ponto_Acesso = %s, Plano_Acesso = %s, 
        Valor_Plano = %s, Vendedor = %s, Data_Adesao = %s, Data_Cancelamento = %s, Status_Contrato = %s, 
        Motivo_Cancelamento = %s WHERE Cod_Contrato = %s
    """
    
    for index, row in df_atualizar.iterrows():
        valores = (
            row['Valor_Contrato_novo'], row['Cod_Cliente_novo'], row['Cliente_novo'], row['Tipo_Pessoa_novo'], row['Celular_novo'], row['Telefone_novo'], 
            row['Email_novo'], row['Vigencia_novo'], row['Cidade_novo'], row['Bairro_novo'], row['Proximo_Reajuste_novo'], row['Ponto_Acesso_novo'], 
            row['Plano_Acesso_novo'], row['Valor_Plano_novo'], row['Vendedor_novo'], row['Data_Adesao_novo'], row['Data_Cancelamento_novo'], 
            row['Status_Contrato_novo'], row['Motivo_Cancelamento_novo'], row['Cod_Contrato']
        )
        
        # Executar a atualização no banco de dados
        ct.executa(sql_update, valores)
    
    print(f"{len(df_atualizar)} registros atualizados com sucesso.")
else:
    print("Nenhum registro para atualizar.")
