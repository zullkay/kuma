import datetime

from celery.task import task

from kuma.metrics.mappings import BaseMetricDocType, INDEX
from kuma.metrics.connection import connection as es


@task
def index_metrics(date_str):
    """
    Given `date` in a string of format 'YYYY-MM-DD', index each metric for that
    date.
    """
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

    for mapping in BaseMetricDocType.__subclasses__():
        doc = mapping.build_document(date)
        es.index(index=INDEX, doc_type=mapping._doc_type.name, body=doc)
