from datetime import date
import pandas as pd

PERIODOS = pd.date_range('2020-01-01',date.today().strftime("%Y-%m-%d"), 
              freq='MS').strftime("%Y%m").tolist()
INPC_ENDPOINT = {
    "agregados": "7063",
    "periodos": "|".join(PERIODOS),
    "variaveis": "44"
}
INPC_PARAMS = {
    "localidades": "N1[all]",
    "classificacao": "315[7169]"
}
INPC_URL = "https://servicodados.ibge.gov.br/api/v3" + '/agregados/' + INPC_ENDPOINT["agregados"] + '/periodos/' + INPC_ENDPOINT['periodos'] + '/variaveis/' + INPC_ENDPOINT["variaveis"]
IPCA_ENDPOINT = {
    "agregados": "7060",
    "periodos": "|".join(PERIODOS),
    "variaveis": "63"
}
IPCA_PARAMS = {
    "localidades": "N1[all]",
    "classificacao": "315[7169]"
}
IPCA_URL = "https://servicodados.ibge.gov.br/api/v3" + '/agregados/' + IPCA_ENDPOINT["agregados"] + '/periodos/' + IPCA_ENDPOINT['periodos'] + '/variaveis/' + IPCA_ENDPOINT["variaveis"]