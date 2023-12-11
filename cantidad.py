import psycopg2

try:
    # Establecer la conexión a la base de datos
    connection = psycopg2.connect(
      host='localhost',
        user='postgres',
        password='password',
        database='peliculas'
    )

    # Crear un objeto cursor
    with connection.cursor() as cursor:
        # Escribir la consulta SQL para contar las filas en la tabla
        table_name = 'ratings'  # Reemplaza con el nombre real de tu tabla
        count_query = f"SELECT COUNT(*) FROM {table_name}"

        # Ejecutar la consulta
        cursor.execute(count_query)

        # Obtener el resultado
        row_count = cursor.fetchone()[0]

        # Imprimir el resultado
        print(f"La tabla {table_name} tiene {row_count} filas.")

except Exception as ex:
    print("Error:", ex)

finally:
    # Cerrar la conexión
    if connection:
        connection.close()
