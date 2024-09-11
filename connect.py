import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def conecta():
    load_dotenv()
    hostEnv = os.getenv("host")
    userEnv = os.getenv("user")
    passwordEnv = os.getenv("password")
    dbEnv = os.getenv("db")

    # Formatar a string de conexão com SQLAlchemy
    engine = create_engine(f'mysql+mysqlconnector://{userEnv}:{passwordEnv}@{hostEnv}/{dbEnv}')
    
    return engine

def executa(instrucao, valores=None):
    engine = conecta()
    with engine.connect() as connection:
        if valores:
            connection.execute(instrucao, valores)
        else:
            connection.execute(instrucao)
