# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 22:32:12 2023

@author: Stats
"""

import pandas as pd
import numpy as np
import os

os.chdir(r'F:\Laboral 2023\central')
os.listdir()

datos = pd.read_csv('Lucy.csv')


# Leer indicando la clase de la columna

datos = pd.read_csv('Lucy.csv', dtype={
    'ID': str,
    'Ubication': str,
    'Level': str,
    'Zone': str,
    'Income': float,
    'Employees': int,
    'Taxes': float,
    'SPAM': str
})

# Seleccionar una base de datos reducidos
df = datos[['ID', 'Level', 'Income']]


# Como renombrar variables
df = datos[['ID', 'Level', 'Income']]


df = df.rename(columns={'Level': 'tamano', 'Income': 'ingreso'})


# Ordenar por tamaño e ingreso

df = df.sort_values(by=['tamano', 'ingreso'], ascending=[True, False])


# Si quisieramos filtrar por las empresas pequeñas con la base de datos reducida y con variables renombradas a español

df2 = datos[['ID', 'Level', 'Income']] \
       .rename(columns={'Level': 'tamano', 'Income': 'ingreso'}) \
       .sort_values(by='ingreso', ascending=False) \
       .query('tamano == "Small"') \
       .drop(columns=['tamano'])
       
# Otra alternativa es utilizar utilizar el método loc

df2 = (datos[['ID', 'Level', 'Income']] \
       .rename(columns={'Level': 'tamano', 'Income': 'ingreso'}) \
       .sort_values(by='ingreso', ascending=False) \
       .loc[df['tamano'] == 'Small'] \
       .drop(columns=['tamano']))   

    
   
    
    
# Calcular los datos agregados por tamaño de la empresa    
def cv(x):
    return(np.std(x) / np.mean(x) * 100)    
    
consulta = datos.groupby(datos['Level']).agg(prom_ingreso=('Income', np.mean),
desv_ingreso = ('Income', np.std), cv_ingreso = ('Income', cv))
consulta.reset_index(inplace = True)


# Parentesis

# Create a Series of integers
s = pd.Series([1, 2, 3, 4, 5])

# Define a function that squares its input
def cuadrados(x):
    return x**2

# Apply the function to each element of the Series
s_cuadrados = s.apply(cuadrados)

# Print the squared Series
print(s_cuadrados)

# También se pueden usar expresiones lambda:

    
# Create a Series of strings
s = pd.Series(['apple', 'banana', 'cherry'])

# Apply the lambda function to each element of the Series
s_length = s.apply(lambda x: len(x))

# Print the length Series
print(s_length)


# Con un dataframe

# Create a DataFrame of numbers
df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})

# Define a function that sums its input
def sum_row(row):
    return sum(row)

# Apply the function to each row of the DataFrame
df_summed_rows = df.apply(sum_row, axis=1) # 1 por fila, 0 por columna

# Print the summed row DataFrame
print(df_summed_rows)



    
# Alternativamente puede agregarse así:

consulta2 = datos.groupby('Level').apply(lambda x: pd.Series({
    'prom_ingreso': np.mean(x['Income']),
    'desv_ingreso': np.std(x['Income']),
    'cv_ingreso': np.std(x['Income']) / np.mean(x['Income']) * 100
}))    
    
# Sin agregar:
    
consulta_noAgregar = pd.DataFrame(pd.Series({
    'prom_ingreso': np.mean(datos['Income']),
    'desv_ingreso': np.std(datos['Income']),
    'cv_ingreso': np.std(datos['Income']) / np.mean(datos['Income']) * 100
})).T

# La T es la transposición


# Crear nuevas variables, ingreso por empleado y ver la empresa con más productividad:
datos['ingresoXempleado'] = datos['Income'] / datos['Employees']
datos = datos.sort_values('ingresoXempleado', ascending = False)




# Otra manera de agregar
datos['temp'] = 'Global'
consulta2 = datos.groupby(datos['temp']).agg(prom_ingreso=('Income', np.mean),
desv_ingreso = ('Income', np.std), cv_ingreso = ('Income', cv))

consulta2.reset_index(inplace = True)
consulta2 = consulta2.rename(columns={'temp': 'Level'})

consulta = datos.groupby(datos['Level']).agg(prom_ingreso=('Income', np.mean),
desv_ingreso = ('Income', np.std), cv_ingreso = ('Income', cv))
consulta.reset_index(inplace = True)
# Pegado por debajo
consulta3 = pd.concat([consulta, consulta2], axis=0)


# Pivotear variables

# Sacar el ingreso por zona y tamaño y pivotearlo, en filas colocar las zonas y en las columnas el tamaño:
consulta4 = datos.groupby(['Zone', 'Level']).agg(prom_ingreso=('Income', np.mean)) 

    
consulta5 = consulta4.pivot_table(values='prom_ingreso', index=['Zone'], columns='Level')
consulta5.reset_index(inplace = True)

# Revisemos como se puede revertir
consulta5 = consulta5.rename(columns={'Big': 'Level_Big', 
                                      'Medium': 'Level_Medium',
                                      'Small': 'Level_Small'})




