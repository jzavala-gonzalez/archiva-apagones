# archiva-apagones

## Environment variables requeridas

Seguir las [instrucciones](https://duckdb.org/docs/guides/import/s3_import#cloudflare-r2) para S3 import usando Cloudflare R2 en los docs de DuckDB. Llenar las siguientes env variables en el archivo `.env`.

AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
DUCKDB_S3_ENDPOINT
AWS_DEFAULT_REGION="auto"

## Workflow configuration

Estos son pasos necesarios para habilitar cualquier software a pedir una archivación a través de Github Actions.

To list workflows and their IDs:

```bash

curl -L \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer MYTOKENHERE" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/jzavala-gonzalez/archiva-apagones/actions/workflows
```

Current IDs:
1. Regions without service - 79830112
2. Genera - Data source - 80504461
3. Outage - Towns - 80759469

To trigger workflow run:

Ej. para regions without service:
```bash
curl -L \
-X POST \
-H "Accept: application/vnd.github+json" \
-H "Authorization: Bearer MYTOKENHERE" \
-H "X-GitHub-Api-Version: 2022-11-28" \
https://api.github.com/repos/jzavala-gonzalez/archiva-apagones/actions/workflows/79830112/dispatches \
-d '{"ref":"main"}'
```

El archivo crudo será subido a la cubeta R2.