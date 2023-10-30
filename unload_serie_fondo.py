from function_unload import *
import pandas as pd
import pg8000

def main():
    # Parámetros de conexión a la base de datos, aqui se debe modificar
    db_params = {
        'database': 'ffmm',
        'user': 'postgres',
        'password': '1123',
        'host': 'localhost',
        'port': 5432
    }

    # Realizar la conexión a la base de datos
    connection = pg8000.connect(**db_params)

    funds=['10000']

    # El tipo 14 es la url respecto a las series del fondo y el 1 es respecto al detalle del fondo
    types_url=['14','1']

    for fund in funds:
        for type_url in types_url:
            df=get_html(fund,type_url)
            print(df)
            if type_url=='14':
                df=transform_df_serie(df,fund)
                insert_tb_series(df,connection)   
            elif type_url=='1':
                df=transform_df_detalle_fondo(df,fund) 
                insert_tb_detalle_fondo(df,connection)
    print("Proceso Finalizado")

if __name__ == "__main__":
    main()
