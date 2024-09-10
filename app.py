import data
import dash
import streamlit as st
import queries

st.title("Leads que se tornaram clientes")
df = data.conversaoLista().reset_index(drop=True)
st.dataframe(df)

st.header("Receita bruta total")
st.write(f"A receita bruta total MENSAL vinda do tráfego pago para este mês é: R$ {dash.receitaBruta()}")

st.header("Pronto para inclusão em banco de dados")
st.write(data.normalizaDados(data.limpaValores(data.conversaoLista())))

st.write(queries.queryGeralRelatorioVendas())