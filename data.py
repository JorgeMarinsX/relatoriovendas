import pandas as pd
from datetime import datetime
import locale
import queries

def conversaoLista():
    vendasDoMes = queries.pathData('vendas')
    leadsDoMes = queries.pathData('leads')

# Substituir strings vazias ou espaços em branco por NaN na coluna 'Cod Contrato'
    vendasDoMes['Cod_Contrato'] = vendasDoMes['Cod_Contrato'].replace(['', ' ', 'nan'], pd.NA)

# Mesclar os dois DataFrames com base na coluna de e-mail
    dfMerged = pd.merge(leadsDoMes, vendasDoMes, on='Email', how='left')

# Filtrar apenas os leads que viraram clientes, ou seja, aqueles que têm o 'Cod Contrato' preenchido
    dfLeadsClientes = dfMerged[dfMerged['Cod_Contrato'].notna()]

# Remover as colunas desnecessárias
    dfLeadsClientes = dfLeadsClientes.drop(columns=['Nome', 'Telefone_x', 'Celular_x', 'Data_Cancelamento'])

    return dfLeadsClientes

#Funções que tratam e normalizam os dados
def limpaValores(dados):
    df = dados
    values = df['Valor_Contrato']
    numberValues = values.replace({'R\$': '', ',': '.'}, regex=True)
    return numberValues

def limpaValoresVendas(dados):
    # Remover espaços em branco no início/fim, remover o símbolo "R$", remover separadores de milhar (vírgula)
    # e garantir que o separador decimal (ponto) seja mantido
    dados['Valor Contrato'] = dados['Valor Contrato'].str.strip() \
                                                       .str.replace(r'R\$', '', regex=True) \
                                                       .str.replace(',', '', regex=True) \
                                                       .str.replace(r'^\s*$', '0', regex=True)  # Substituir strings vazias por 0
    dados['Valor Contrato'] = dados['Valor Contrato'].astype(float)
    
    dados['Valor Plano'] = dados['Valor Plano'].str.strip() \
                                               .str.replace(r'R\$', '', regex=True) \
                                               .str.replace(',', '', regex=True) \
                                               .str.replace(r'^\s*$', '0', regex=True)  # Substituir strings vazias por 0
    dados['Valor Plano'] = dados['Valor Plano'].astype(float)

    # Verificar os valores após a transformação
    print("Valores após transformação:\n", dados[['Valor Contrato', 'Valor Plano']].head())

    return dados

#Usar apenas para definir novos dados, 
# os parâmetros usam o agora

def adicionaMesAno(df):
    # Definir o local como português do Brasil
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
    # Obter o mês e o ano atuais
    mes_atual = datetime.now().strftime('%m')  # Formato MM
    ano_atual = datetime.now().strftime('%Y')  # Formato YYYY

    # Adicionar as colunas "Mês" e "Ano" ao início do DataFrame
    df.insert(0, 'Mês', mes_atual)
    df.insert(1, 'Ano', ano_atual)

    return df

def converte_uppercase(df):
    # Aplicar str.upper() em todas as colunas que contêm strings
    df = df.apply(lambda col: col.map(lambda x: x.upper() if isinstance(x, str) else x))
    return df

def normalizaDados(dados):
    resultado = converte_uppercase(dados.reset_index(drop=True))
    resultado = resultado.dropna(subset=['Cod Cliente'])
    return resultado