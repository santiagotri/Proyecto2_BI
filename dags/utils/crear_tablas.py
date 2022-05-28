def crear_tablas():
    return """
        DROP TABLE IF EXISTS fact_registro, departamento, fecha, tipo, entidad;

        CREATE TABLE IF NOT EXISTS fecha(
            Fecha_Key INT PRIMARY KEY,
            Anio INT,
            Mes INT
        );

        CREATE TABLE IF NOT EXISTS departamento(
            Codigo INT PRIMARY KEY,
            Nombre VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS entidad(
            Codigo INT PRIMARY KEY,
            Nombre VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS tipo(
            Tipo_Key INT PRIMARY KEY,
            Subcategoria VARCHAR(150),
            Indicador VARCHAR(150),
            Categoria VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS fact_registro(
            Departamento INT REFERENCES departamento(Codigo),
            Fecha INT REFERENCES fecha(Fecha_Key),
            Entidad INT REFERENCES entidad(Codigo),
            Tipo INT REFERENCES tipo(Tipo_Key)
            Dato_Numerico INT,
            Dato_Cualitativo VARCHAR(150),
            Unidad_;edida VARCHAR(150)
        );
    """
