import pandas as pd
import pymysql 
import time
from pymysql.cursors import DictCursor
from datetime import datetime

ruta_excel = r"C:\python proyect\1. Divipola\data\DIVIPOLA_Municipios.xlsx";

def conexion(sentencia, valores):
    conexion = pymysql.connect(
        host = 'localhost',
        user = 'root',
        passwd = 'root',
        database = 'hrm_desa_test',
        port =  3306,
        cursorclass = DictCursor
    )

    try:
        with conexion.cursor() as cursor:
            cursor.execute(sentencia, valores)
            
            conexion.commit()                     # <-- commit aquí
            return cursor.lastrowid 
    finally:
        # 3. Cierra la conexión
        conexion.close()
        print("Conexión PyMySQL cerrada.")

def sentenciaInsertData(name_table):
    sentencia_slq =  f"""INSERT INTO {name_table} (code, name, code_large, country, departament, longitud, Latitud, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    return sentencia_slq

def createData(sentencia, valores):
    conexion(sentencia, valores)


def correrProceso():
    pf = pd.read_excel(ruta_excel, sheet_name='document', usecols="A:G", nrows=1123);

    for i, file in pf.iterrows():

        print('/* ******************************************* */')
        print("< Iniciando proceso...")

        valores = (
            f'{file.code}',
            f'{file.name}',
            f'{file.code_large}',
            f'{file.country}',
            f'{file.departament}',
            f'{file.length}',
            f'{file.latitude}',
            datetime.now(),
            datetime.now(),
        )

        sentencia = sentenciaInsertData('ubiegos_colombia')

        createData(sentencia, valores)

        if createData:
            print("Data creada con exito")
        else:
            print(f'Validar codigo {file.code_large}')

    print("Proceso finalizado")
    print('/* ******************************************* */')

if ruta_excel:
    print('El documeto existe')

    correrProceso()
else:
    print('El archivo no existe en el fichero')




print("debug")

