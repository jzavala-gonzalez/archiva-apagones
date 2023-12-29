import os
import boto3
import json
import requests
from dotenv import load_dotenv
from utils import get_atc_now

# Para archivar la copia actual de regiones sin servicio

load_dotenv()  # take environment variables from .env.

timestamp_saved_datetime = get_atc_now()
timestamp_saved = timestamp_saved_datetime.strftime("%Y-%m-%dT%H-%M-%S%z")
date_saved = timestamp_saved_datetime.strftime("%Y-%m-%d")
print('timestamp saved:', timestamp_saved)

# Get the data from the API
url = 'https://api.miluma.lumapr.com/miluma-outage-api/outage/municipality/towns'
r = requests.post(url, json=(
    [
		"ADJUNTAS",
		"AGUADA",
		"AGUADILLA",
		"AGUAS BUENAS",
		"AIBONITO",
		"ARECIBO",
		"ARROYO",
		"ANASCO",
		"AÑASCO",
		"BARCELONETA",
		"BARRANQUITAS",
		"BAYAMON",
		"BAYAMÓN",
		"CABO ROJO",
		"CAGUAS",
		"CAMPANILLA",
		"CAMUY",
		"CANDELARIA",
		"CANDELARIA ARENAS",
		"CANOVANAS",
		"CANÓVANAS",
		"CAROLINA",
		"CATANO",
		"CATAÑO",
		"CAYEY",
		"CEIBA",
		"CIALES",
		"CIDRA",
		"COAMO",
		"COCO",
		"COMERIO",
		"COMERÍO",
		"COROZAL",
		"CULEBRA",
		"DORADO",
		"ESTANCIAS DE FLORIDA",
		"FAJARDO",
		"FLORIDA",
		"GUANICA",
		"GUAYAMA",
		"GUAYANILLA",
		"GUAYNABO",
		"GURABO",
		"GUANICA",
		"GUÁNICA",
		"HATILLO",
		"HORMIGUEROS",
		"HUMACAO",
		"INGENIO",
		"ISABEL SEGUNDA",
		"ISABELA",
		"JAYUYA",
		"JUANA DIAZ",
		"JUANA DÍAZ",
		"JUNCOS",
		"LAJAS",
		"LARES",
		"LAS MARIAS",
		"LAS MARÍAS",
		"LAS PIEDRAS",
		"LEVITTOWN",
		"LOIZA",
		"LOÍZA",
		"LUQUILLO",
		"MANATI",
		"MANATÍ",
		"MARICAO",
		"MAUNABO",
		"MAYAGUEZ",
		"MAYAGÜEZ",
		"MOCA",
		"MOROVIS",
		"NAGUABO",
		"NARANJITO",
		"OROCOVIS",
		"PAJAROS",
		"PATILLAS",
		"PENUELAS",
		"PEÑUELAS",
		"PONCE",
		"PUERTO REAL",
		"PUNTA SANTIAGO",
		"QUEBRADILLAS",
		"RINCON",
		"RINCÓN",
		"RIO GRANDE",
		"RÍO GRANDE",
		"SABANA GRANDE",
		"SABANA SECA",
		"SALINAS",
		"SAN ANTONIO",
		"SAN GERMAN",
		"SAN GERMÁN",
		"SAN ISIDRO",
		"SAN JUAN",
		"HATO REY",
		"PUERTO NUEVO",
		"RIO PIEDRAS",
		"RÍO PIEDRAS",
		"SANTURCE",
		"SAN LORENZO",
		"SAN SEBASTIAN",
		"SAN SEBASTIÁN",
		"SANTA BARBARA",
		"SANTA ISABEL",
		"TOA ALTA",
		"TOA BAJA",
		"TRUJILLO ALTO",
		"UTUADO",
		"VEGA ALTA",
		"VEGA BAJA",
		"VIEQUES",
		"VILLALBA",
		"YABUCOA",
		"YAUCO"
	]
))
print(r)
print(r.headers)
r.raise_for_status()
print()

if r.headers['Content-Type'] != 'application/json':
    raise Exception('Unexpected content type: ' + r.headers['Content-Type'])

outage_towns_dict = r.json()

outage_towns_json_string = json.dumps(outage_towns_dict, indent=2, sort_keys=True)
print('outage_towns_json_string:')
print(outage_towns_json_string)
print()

# print('Probando desde boto3:')
s3 = boto3.client('s3',
    endpoint_url='https://'+os.getenv('DUCKDB_S3_ENDPOINT'),
)

filekey = f'outage_towns/{date_saved}/outage_towns__{timestamp_saved}.json'
print(f"Saving to R2 at '{filekey}'")
s3.put_object(
    Bucket='archiva-apagones',
    Key=filekey,
    Body=outage_towns_json_string,
)