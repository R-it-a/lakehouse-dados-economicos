"""Assets compartilhados entre as DAGs (agendamento orientado a dados do Airflow 3)."""

from airflow.sdk import Asset

SILVER_SGS = Asset("lakehouse://silver/bcb_sgs")
SILVER_SIDRA = Asset("lakehouse://silver/ibge_sidra")
GOLD_PAINEL = Asset("lakehouse://gold/painel_mensal")
