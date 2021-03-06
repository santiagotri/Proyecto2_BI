from pandas import DataFrame
from utils.file_util import cargar_datos

def insert_query_educacion_fecha(**kwargs):
    
    insert = f"INSERT INTO fecha (Anio,Mes) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Anio},{row.Mes})"
        insertQuery += insert + f" WHERE NOT EXISTS (SELECT * FROM fecha WHERE Anio={row.Anio} AND Mes={row.Mes});\n"
    return insertQuery

def insert_query_educacion_entidad(**kwargs):
    
    insert = f"INSERT INTO entidad (Codigo,Nombre) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Codigo_Entidad},\'{row.Entidad}\')"
        insertQuery += insert + f" WHERE NOT EXISTS (SELECT * FROM entidad WHERE Codigo={row.Codigo_Entidad} AND Nombre=\'{row.Entidad}\');\n"
    return insertQuery

def insert_query_educacion_tipo(**kwargs):
    
    insert = f"INSERT INTO tipo (Subcategoria,Indicador,Categoria) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(\'{row.Subcategoria}\',\'{row.Indicador}\',\'{row.Categoria}\')"
        insertQuery += insert + f" WHERE NOT EXISTS (SELECT * FROM tipo WHERE Subcategoria=\'{row.Subcategoria}\' AND Indicador=\'{row.Indicador}\' AND Categoria=\'{row.Categoria}\');\n"
    return insertQuery

def insert_query_educacion_departamento(**kwargs):
    insert = f"INSERT INTO departamento (Codigo,Nombre) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Codigo_Departamento},\'{row.Departamento}\');\n"
        insertQuery += insert + f" WHERE NOT EXISTS (SELECT * FROM departamento WHERE Codigo_Departamento={row.Codigo_Departamento} AND Departamento=\'{row.Departamento}\');\n"
    return insertQuery

def insert_query_educacion_fact(**kwargs):
    
    insert = f"INSERT INTO fact_registro (Departamento,Fecha,Entidad,Tipo,Dato_Numerico,Dato_Cualitativo,Unidad_Medida) VALUES "
    insertQuery = ""
    valueTipo=f'(SELECT Tipo_Key FROM tipo WHERE Subcategoria=\'{row.Subcategoria}\' AND Indicador=\'{row.Indicador}\' AND Categoria=\'{row.Categoria}\')'
    valueFecha=f'(SELECT Fecha_Key FROM fecha WHERE Anio={row.Anio} AND Mes={row.Mes})'
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Codigo_Departamento},{valueFecha},{row.Codigo_Entidad},{valueTipo},{row.Dato_Numerico},\'{row.Dato_Cualitativo}\',\'{row.Unidad_Medida}\');\n"
    return insertQuery

# customer insertion
def insert_query_customer(**kwargs):
    # To Do
    insert= f"INSERT INTO customer (Anio,Mes) VALUES "
    insertQuery = ""
    dataframe = cargar_datos(kwargs["csv_path"])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Customer_Key},\'{row.Customer}\',\'{row.Bill_To_Customer}\',\'{row.Category}\',\'{row.Buying_Group}\',\'{row.Primary_Contact}\',{row.Postal_Code});\n"
    return insertQuery

# date insertion
def insert_query_date(**kwargs):
    # To Do
    insert= f"INSERT INTO date_table (Date_key,Day_Number,Day_val,Month_val,Short_Month,Calendar_Month_Number,Calendar_Year,Fiscal_Month_Number,Fiscal_Year) VALUES "
    insertQuery = ""
    dataframe = cargar_datos(kwargs["csv_path"])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"(TO_DATE(\'{row.Date_key}\', \'YYYY-MM-DD\'),{row.Day_Number},{row.Day_val},\'{row.Month_val}\',\'{row.Short_Month}\',{row.Calendar_Month_Number},{row.Calendar_Year},{row.Fiscal_Month_Number},{row.Fiscal_Year});\n"
    return insertQuery

# employee insertion
def insert_query_employee(**kwargs):
    # To Do
    insert= f"INSERT INTO employee (Employee_Key,Employee,Preferred_Name,Is_Salesperson) VALUES "
    insertQuery = ""
    dataframe = cargar_datos(kwargs["csv_path"])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Employee_Key},\'{row.Employee}\',\'{row.Preferred_Name}\',\'{row.Is_Salesperson}');\n"
    return insertQuery

# stock item insertion
def insert_query_stock(**kwargs):
    # To Do 
    insert= f"INSERT INTO stockitem (Stock_Item_Key,Stock_Item,Color,Selling_Package,Buying_Package,Brand,Size_val,Lead_Time_Days,Quantity_Per_Outer,Is_Chiller_Stock,Tax_Rate,Unit_Price,Recommended_Retail_Price,Typical_Weight_Per_Unit) VALUES "
    insertQuery = ""
    dataframe = cargar_datos(kwargs["csv_path"])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.Stock_Item_Key},\'{row.Stock_Item}\',\'{row.Color}\',\'{row.Selling_Package}\',\'{row.Buying_Package}\',\'{row.Brand}\',\'{row.Size_val}\',\'{row.Lead_Time_Days}\',\'{row.Quantity_Per_Outer}\',\'{row.Is_Chiller_Stock}\',\'{row.Tax_Rate}\',\'{row.Unit_Price}\',\'{row.Recommended_Retail_Price}\',\'{row.Typical_Weight_Per_Unit}');\n"
    return insertQuery
    
# fact order insert
def insert_query_fact_order(**kwargs):
    insert= f"INSERT INTO fact_order (order_key,city_key,customer_key,stock_item_key,order_date_key,picked_date_key,salesperson_key,picker_key,package,quantity,unit_price,tax_rate,total_excluding_tax,tax_amount,total_including_tax) VALUES "
    insertQuery = ""
    dataframe = cargar_datos(kwargs["csv_path"])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.order_key},\'{row.city_key}\',\'{row.customer_key}\',\'{row.stock_item_key}\',\'{row.order_date_key}\',\'{row.picked_date_key}\',\'{row.salesperson_key}\',\'{row.picker_key}\',\'{row.package}\',\'{row.quantity}\',\'{row.unit_price}\',\'{row.tax_rate}\',\'{row.total_excluding_tax}\',\'{row.tax_amount}\',{row.total_including_tax});\n"
    return insertQuery