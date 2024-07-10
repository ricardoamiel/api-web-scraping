import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json

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

    # Extraer los encabezados de la tabla, omitiendo el primero (índice)
    headers = [header.text.strip() for header in table.find_all('th')[1:]]

    # Extraer las filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        index_cell = row.find('th')
        if len(cells) > 0 and index_cell:  # Verificar que haya celdas y un índice
            row_data = {'#': index_cell.text.strip()}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.text.strip()
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
        'headers': ['#'] + headers,
        'rows': rows
    }

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
