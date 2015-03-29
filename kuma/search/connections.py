from django.conf import settings
from elasticsearch_dsl.connections import connections


# Configure Elasticsearch connections for connection pooling.
connections.configure(
    default={'hosts': settings.ES_URLS},
    indexing={'hosts': settings.ES_URLS,
              'timeout': settings.ES_INDEXING_TIMEOUT},
)
es = connections.get_connection('default')
