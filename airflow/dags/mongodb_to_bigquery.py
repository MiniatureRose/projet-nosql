import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)

def preprocess_and_insert_to_bigquery():
    import pandas as pd
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    from pymongo import MongoClient


    # Get the absolute path to the service account JSON file
    service_account_path = "./data/service-account.json"

    # Connexion à MongoDB
    print("Connecting to MongoDB...")
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['movies_database']
    collection = db['posts']

    # Récupération des données depuis MongoDB
    print("Fetching data from MongoDB...")
    data = list(collection.find({}))

    # Création d'un DataFrame pandas à partir des données MongoDB
    print("Creating DataFrame from MongoDB data...")
    df = pd.DataFrame(data)

    # Exemple d'opération de pré-agrégation : Calculer la somme du Score par PostTypeId
    df['@Score'] = df['@Score'].astype(int)
    aggregated_data = df.groupby('@PostTypeId')['@Score'].sum().reset_index()
    aggregated_data = aggregated_data.rename(columns={'@PostTypeId': 'PostTypeId'})
    aggregated_data = aggregated_data.rename(columns={'@Score': 'Score'})
    print(f"Aggregated data: {aggregated_data}")


    # Connexion à BigQuery
    print("Connecting to BigQuery...")
    bq_client = bigquery.Client.from_service_account_json(service_account_path)
    dataset_id = 'movies_database'
    table_id = 'posts'

    # Vérifier si le dataset existe, sinon le créer
    print("Checking BigQuery dataset...")
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)

    # Vérifier si la table existe, sinon la créer
    print("Checking BigQuery table...")
    table_ref = dataset_ref.table(table_id)
    try:
        bq_client.get_table(table_ref)
    except NotFound:
        # Créer la table avec un schéma approprié
        schema = [
            bigquery.SchemaField('PostTypeId', 'INTEGER'),
            bigquery.SchemaField('Score', 'INTEGER'),
            # Ajoutez d'autres champs selon vos besoins
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)

    # Créer ou écraser la table dans BigQuery
    print("Creating or replacing BigQuery table...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Charger les données agrégées dans BigQuery
    print("Loading data into BigQuery...")
    job = bq_client.load_table_from_dataframe(aggregated_data, f'{dataset_id}.{table_id}', job_config=job_config)
    job.result()  # Attendre que le job soit terminé
    print("Job completed successfully")


with DAG(
    dag_id="mongodb_to_bigquery",
    schedule=timedelta(minutes=30), #30 minutes 
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    # Tâche Airflow pour la fonction de pré-agrégation et d'insertion dans BigQuery
    insert_task = PythonVirtualenvOperator(
        task_id='preprocess_and_insert_to_bigquery',
        requirements=["pymongo", "pandas", "google-cloud-bigquery"],
        python_callable=preprocess_and_insert_to_bigquery,
        dag=dag,
    )
