import os

from google.api_core.client_options import ClientOptions

from google.cloud import bigquery
from google.cloud import documentai
from google.cloud import storage

project_id = 'example'
location = 'us'
mime_type = 'application/pdf'
processor_id = '1234567890'
bucket_name = 'example'
dataset_id = 'research_papers'
table_id = 'parsed_text'

def parse_to_bigquery(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")

    local_path = "/tmp/" + file['name']

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file['name'])
    blob.download_to_filename(local_path)

    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)
    name = client.processor_path(project_id, location, processor_id)
    with open(local_path, "rb") as image:
        image_content = image.read()
    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    document = result.document

    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    rows_to_insert = [
        {"file": file['name'], "text": document.text},
    ]

    errors = client.insert_rows_json(table, rows_to_insert)

    if errors == []:
        print("The parsed PDF data has been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))