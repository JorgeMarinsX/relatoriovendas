import connect as ct
import pandas as pd

def pathData(data):
    if (data == 'vendas'):
        df =  pd.read_sql('SELECT * FROM `vendas`', con=ct.conecta())
        return df
    elif (data == 'leads'):
        df = pd.read_sql('SELECT * FROM `leads`', con=ct.conecta())
        return df


def queryGeralRelatorioVendas():
    df = pd.read_sql('SELECT * FROM `relatorioconversaogeral`', con=ct.conecta())
    return df

#Cada função representa uma query