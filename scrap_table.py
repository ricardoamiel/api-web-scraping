import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla
    url = "https://sgonorte.bomberosperu.gob.pe/24horas/?criterio=/"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla en el HTML
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer los encabezados de la tabla, omitiendo el primero
    headers = [header.text.strip() for header in table.find_all('th')[1:]]

    # Mapear los encabezados a los nombres de las columnas en DynamoDB
    headers_map = {
        'Nro Parte': 'NroParte',
        'Fecha y hora': 'FechaYHora',
        'Dirección / Distrito': 'DireccionDistrito',
        'Tipo': 'Tipo',
        'Estado': 'Estado',
        'Máquinas': 'Maquinas',
        'Ver Mapa': 'VerMapa'
    }

    # Extraer las filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all(['td', 'th'])
        if len(cells) > 1:  # Asegurar que hay más de una celda
            row_data = {'#': cells[0].text.strip()}
            row_data.update({headers_map[headers[i]]: cells[i + 1].text.strip() for i in range(len(headers))})
            row_data['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
            rows.append(row_data)

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos
    for row in rows:
        table.put_item(Item=row)

    # Construir el resultado
    result = {
        'headers': ['#'] + list(headers_map.values()),
        'rows': rows
    }

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': result
    }
