import datetime
from optparse import make_option

from django.core.management.base import BaseCommand, CommandError
from elasticsearch.exceptions import NotFoundError

from kuma.metrics.connection import connection as es
from kuma.metrics.mappings import BaseMetricDocType, INDEX
from kuma.metrics.tasks import index_metrics


class Command(BaseCommand):
    help = 'Create or update MDN metrics'

    option_list = BaseCommand.option_list + (
        make_option('-s', '--start', type='string', dest='start',
                    help='Start date in YYYY-MM-DD format.'),
        make_option('-e', '--end', type='string', dest='end',
                    help='End date in YYYY-MM-DD format.'),
        make_option('-p', '--purge', dest='purge', default=False,
                    action='store_true', help='Delete index and recreate.')
    )

    def handle(self, *args, **options):
        if options['start'] is None:
            raise CommandError('Start date required')
        if options['end'] is None:
            raise CommandError('Ending date required')

        try:
            start = datetime.datetime.strptime(
                options['start'], '%Y-%m-%d').date()
            end = datetime.datetime.strptime(
                options['end'], '%Y-%m-%d').date()
        except ValueError as e:
            raise CommandError(e)

        if options['purge']:
            try:
                es.indices.delete(index=INDEX)
            except NotFoundError:
                pass  # It doesn't exist, no problem.

        # Check if index exists and if not create it.
        if not es.indices.exists(index=INDEX):
            es.indices.create(index=INDEX,
                              body={'settings': {'number_of_shards': 1,
                                                 'number_of_replicas': 1}})

        # Check if mappings exist and if not create it.
        for mapping in BaseMetricDocType.__subclasses__():
            doc_type = mapping._doc_type.name
            if not es.indices.exists_type(index=INDEX, doc_type=doc_type):
                es.indices.put_mapping(index=INDEX, doc_type=doc_type,
                                       body=mapping.get_mapping())

        # Iterate over dates and index metrics, one task per date.
        days = range(0, (end - start).days + 1)
        for date in (start + datetime.timedelta(days=i) for i in days):
            index_metrics.delay(date.isoformat())
            self.stdout.write('Spawned task to indexing metrics for date: %s\n'
                              % date.isoformat())
