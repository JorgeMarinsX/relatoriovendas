import os
from dotenv import load_dotenv
import mysql.connector

def conecta():
    load_dotenv()
    hostEnv = os.getenv("host")
    userEnv = os.getenv("user")
    passwordEnv = os.getenv("password")
    dbEnv = os.getenv("db")

    db = mysql.connector.connect(
        host = hostEnv,
        user = userEnv,
        password = passwordEnv,
        database = dbEnv
        )

    return db

def executa(instrucao, valores=None):
    db = conecta()
    cursor = db.cursor()
    if valores:
        cursor.execute(instrucao, valores)
    else:
        cursor.execute(instrucao)
    db.commit()
    cursor.close()
    db.close()

