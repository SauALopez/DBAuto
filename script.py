import pandas as pd
import mariadb
import sys
import numpy as np
try:
    conn = mariadb.connect(
        user="root",
        password="72851100",
        host="localhost",
        port=3306,
        database="zapateria"

    )
    cursor = conn.cursor()
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
    


marcas_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Marcas")

for index, row in marcas_df.iterrows():
    sql = "INSERT INTO marca (nombre) VALUES (?)"
    print(sql)
    cursor.execute(sql, (row["Marca"],))
    print(row["Marca"])

conn.commit()

def marca_id(marca:str):
    cur = conn.cursor()
    cur.execute("SELECT Id_marca FROM marca WHERE nombre=?", (marca,))
    for id_marca, in cur:            
        print(f"Marca {marca} con ID {id_marca}")
        return id_marca
                    
    

sucursales_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Sucursales")

for index, row in sucursales_df.iterrows():
    sql = "INSERT INTO sucursal (nombre, direccion, telefono) VALUES (?, ?, ?)"
    print(sql)
    cursor.execute(sql, (row["Nombre"], row["Dirección"], row["Teléfono"]))
    print(row["Nombre"], row["Dirección"], row["Teléfono"])

conn.commit()

def sucursal_id(sucursal:str):
    cur = conn.cursor()
    cur.execute("SELECT Id_sucursal FROM sucursal WHERE nombre=?", (sucursal,))
    for id_sucursal, in cur:            
        print(f"Sucursal {sucursal} con ID {id_sucursal}")
        return id_sucursal

colores_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Colores")

for index, row in colores_df.iterrows():
    sql = "INSERT INTO color (nombre) VALUES (?)"
    print(sql)
    cursor.execute(sql, (row["Descripcion"],))
    print(row["Descripcion"],)

conn.commit()

def color_id(color:str):
    cur = conn.cursor()
    cur.execute("SELECT Id_color FROM color WHERE nombre=?", (color,))
    for id_color, in cur:            
        print(f"color {color} con ID {id_color}")
        return id_color

modelos_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Modelos")

for index, row in modelos_df.iterrows():
    sql = "INSERT INTO modelo (id_marca, nombre) VALUES (?, ?)"
    id_marca = marca_id(row["Marca"])
    print(sql)
    cursor.execute(sql, (id_marca, row["Modelo"]))
    print(id_marca, row["Modelo"])

conn.commit()    

def modelo_id(modelo:str):
    cur = conn.cursor()
    cur.execute("SELECT Id_modelo FROM modelo WHERE nombre=?", (modelo,))
    for id_modelo, in cur:            
        print(f"modelo {modelo} con ID {id_modelo}")
        return id_modelo

clientes_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Clientes")

for index, row in clientes_df.iterrows():
    try:
        sql = "INSERT INTO cliente (nombre, direccion, nit, telefono, correo, informacion) VALUES (?, ?, ?, ?, ?, ?)"
        print(sql)
        print(row["Nombre"], row["Direcciòn"], row["NIT"], row["Telefono"], row["Correo"], (row["Quiere recibir inofrmacion"]=="Si"))
        cursor.execute(sql, (row["Nombre"], row["Direcciòn"], row["NIT"], row["Telefono"], row["Correo"], (row["Quiere recibir inofrmacion"]=="Si")))
    except Exception as e:
        pass
conn.commit()

proveedores_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Proveedores")

for index, row in proveedores_df.iterrows():
    try:
        sql = "INSERT INTO proveedor (nombre, direccion) VALUES (?, ?)"
        print(sql)
        print(row["Nombre"], row["Direcciòn"])
        cursor.execute(sql, (row["Nombre"], row["Direcciòn"]))
        id_proveedor = cursor.lastrowid
        for marca in row[2:]:
            if type(marca) == str:
                id_marca = marca_id(marca)
                sql = "INSERT INTO proveedormarca (id_proveedor, id_marca) VALUES (?, ?)"
                print(sql)
                print(id_proveedor, id_marca)
                cursor.execute(sql, (id_proveedor, id_marca))
                    
    except Exception as e:
        pass
conn.commit()

diccionario_sucursal_inv = {
    "Existencia en Z1": "Guatemala Zona 1",
    "Existencia en Z9": "Guatemala Zona 9",
    "Existencia en Quetzaltenango": "Quetzaltenango"
}

def insert_inventario(id_sucursal, id_producto, cantidad):
    sql = "INSERT INTO inventario (id_sucursal, id_producto, cantidad) VALUES (?, ?, ?)"

    cursor.execute(sql, (id_sucursal, id_producto, cantidad))

zapapatos_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Zapatos")

for index, row in zapapatos_df.iterrows():
    id_marca = marca_id(row["Marca"])
    id_modelo = modelo_id(row["Modelo"])
    id_color = color_id(row["Color"])

    sql = "INSERT INTO producto (id_marca, id_modelo, talla, id_color) VALUES (?, ?, ?, ?)"
    print(sql)
    print(id_marca, id_modelo, row["Talla"], id_color)
    cursor.execute(sql, (id_marca, id_modelo, row["Talla"], id_color))
    id_producto = cursor.lastrowid

    id_sucursal = sucursal_id(diccionario_sucursal_inv["Existencia en Z1"])
    insert_inventario(id_sucursal, id_producto, row["Existencia en Z1"])

    id_sucursal = sucursal_id(diccionario_sucursal_inv["Existencia en Z9"])
    insert_inventario(id_sucursal, id_producto, row["Existencia en Z9"])  

    id_sucursal = sucursal_id(diccionario_sucursal_inv["Existencia en Quetzaltenango"])
    insert_inventario(id_sucursal, id_producto, row["Existencia en Quetzaltenango"])
conn.commit()

bolsos_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Bolsos")

for index, row in bolsos_df.iterrows():
    id_marca = marca_id(row["Marca"])
    id_modelo = modelo_id(row["Modelo"])
    id_color = color_id(row["Color"])

    sql = "INSERT INTO producto (id_marca, id_modelo, talla, id_color) VALUES (?, ?, ?, ?)"
    print(sql)
    print(id_marca, id_modelo, row["Talla"], id_color)
    cursor.execute(sql, (id_marca, id_modelo, row["Talla"], id_color))
    id_producto = cursor.lastrowid

    id_sucursal = sucursal_id(diccionario_sucursal_inv["Existencia en Z1"])
    insert_inventario(id_sucursal, id_producto, row["Existencia en Z1"])

    id_sucursal = sucursal_id(diccionario_sucursal_inv["Existencia en Z9"])
    insert_inventario(id_sucursal, id_producto, row["Existencia en Z9"])  

    id_sucursal = sucursal_id(diccionario_sucursal_inv["Existencia en Quetzaltenango"])
    insert_inventario(id_sucursal, id_producto, row["Existencia en Quetzaltenango"])
conn.commit()

ventas_df = pd.read_excel("BD Zapateria.xlsx", sheet_name="Ventas")

for index, row in ventas_df.iterrows():
    pass