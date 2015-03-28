from django.conf import settings

from elasticsearch_dsl.connections import connections


connections.configure(default={'hosts': settings.ES_URLS})
connection = connections.get_connection('default')
