import connect as ct
import data
import queries

# Obter os dados limpos da função conversaoLista
df = data.conversaoLista()

# Ler os dados existentes da tabela
df_existente = queries.tabelaConversaoGeral()

# Remover duplicatas com base em uma chave primária (exemplo: 'Cod_Contrato')
df_novos_dados = df[~df['Cod_Contrato'].isin(df_existente['Cod_Contrato'])]

# Inserir os dados do DataFrame no banco de dados
if not df_novos_dados.empty:
    df_novos_dados.to_sql(name="relatorioconversaogeral", con=ct.conecta(), if_exists='append', index=False)
    print("Novos dados inseridos com sucesso na tabela relatorioconversaogeral")
else:
    print("Nenhum dado novo para inserir.")