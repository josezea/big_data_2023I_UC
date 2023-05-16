# -*- coding: utf-8 -*-
"""
Created on Mon May 15 08:14:49 2023

@author: joszea
"""



# Censo 



import os
import polars as pl
os.chdir(r'C:\Users\joszea\Documents\censo 2022\parquet')
os.listdir()

censo_personas = pl.scan_parquet("personas/*.parquet")

# realizar conteos del censo
consulta0 = censo_personas.select(pl.count()).collect()
consulta0 = pl.DataFrame.to_pandas(consulta0)

# Realizar conteo por sexo
consulta1 = censo_personas.groupby('P_SEXO').agg(pl.count()).collect()
pl.DataFrame.to_pandas(consulta1)


consulta1 = censo_personas.groupby('P_SEXO').agg(pl.count().alias('Cuentica')).collect()
pl.DataFrame.to_pandas(consulta1)


# censo_personas.columns
consulta2 = censo_personas.groupby(['U_DPTO', 'U_MPIO']).agg(pl.count().alias('Cuentica')).collect()
consulta2 = pl.DataFrame.to_pandas(consulta2)




# Ejemplos de juguete

import pyarrow
import polars as pl
import os

#os.chdir(r'F:\Laboral 2023\central')
os.chdir(r'C:\Users\joszea\Downloads\data')
os.listdir()



# Read with explicit data types
# Note: Polars does not currently support direct type specification during csv loading
# Instead, use the with_column method to convert types after loading


datos = pl.read_csv('Lucy.csv')
datos.head()
datos.describe()
datos.shape


# Select a subset of the data
df = datos.select(['ID', 'Level', 'Income'])
datos.select(pl.col(['ID', 'Level', 'Income']))
pl.DataFrame.to_pandas(df)

# Totalizar el ingreso total de ingreso
datos.select(['Income']).sum()

# Crear un valor constante
df = df.with_columns(pl.lit("empresas").alias("tipo"))



# calcular productividad
#datos = datos.with_columns(pl.col("Income") / pl.col("Employees")).alias("productividad")


# Ordenar por tamaño e ingreso
#df = df.sort_values(by=['tamano', 'ingreso'], ascending=[True, False])
datos = datos.sort(by=['Level', 'Income'], descending=[False, True])
pl.DataFrame.to_pandas(datos)

# Renombrar variables
# df[['ID', 'Level', 'Income']].rename(columns={'Level': 'tamano', 'Income': 'ingreso'})
df = df.rename({'Level': 'tamano', 'Income': 'ingreso'})

a = pl.DataFrame.to_pandas(df)



# Sacamos agregaciones
os.chdir(r'C:\Users\joszea\Downloads\data')
datos = pl.read_csv('Lucy.csv')

resumen = datos.select(
    [pl.count().alias('N'),
     pl.mean("Income").alias('prom_ingreso'),
     pl.col("Income").quantile(0.25).alias('p25_ingreso'),
     pl.median("Income").alias('mediana_ingreso'),
     pl.col("Income").quantile(0.75).alias('p75_ingreso'),
     pl.mean("Taxes").alias('prom_impuesto'),
    ]
)
resumen = pl.DataFrame.to_pandas(resumen)


# Filtrar
df_small = df.filter((pl.col("tamano") == "Small") & (pl.col("ingreso") <= 10))
df_small = pl.DataFrame.to_pandas(df_small)



# Agregaciones a nivel de una variable categórica:
     
consulta = datos.groupby("Level").agg(
    [
        pl.mean("Income").alias('prom_ingreso'),
        pl.std("Income").alias('desv_ingreso'),
           pl.mean("Taxes").alias('prom_impuestos')
    ]
)







