import data

def receitaBruta():
    valores = data.limpaValores(data.conversaoLista()).astype(float)
    receitaBrutaTotal = valores.sum()
    return receitaBrutaTotal


