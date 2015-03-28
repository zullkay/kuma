import datetime

from elasticsearch_dsl import document, field
from elasticsearch_dsl.mapping import Mapping

from django.contrib.auth.models import User
from django.db.models import Min

from kuma.users.models import UserBan
from kuma.wiki.models import Revision


INDEX = 'mdn-metrics'


class BaseMetricDocType(document.DocType):
    date = field.Date(format='date')
    count = field.Long()

    @classmethod
    def fetch_data(cls, date):
        raise NotImplementedError(cls)

    @classmethod
    def build_document(cls, date, **kwargs):
        """
        Extra kwargs will be stored in the metrics mapping. These can be used
        to include extra dimensions to further filter data.
        """
        doc = {
            'date': date.isoformat(),
        }
        data = cls.fetch_data(date)
        doc.update(data)
        if kwargs:
            doc.update(kwargs)
        return doc

    @classmethod
    def get_mapping(cls):
        return cls._doc_type.mapping.to_dict()

    @classmethod
    def get_settings(cls):
        return {
            'mappings': cls.get_mapping(),
            'settings': {
                'number_of_replicas': 1,
                'number_of_shards': 1,
            }
        }


class TotalUsers(BaseMetricDocType):

    class Meta(object):
        mapping = Mapping('total_users')
        mapping.meta('_all', enabled=False)

    @classmethod
    def fetch_data(cls, date):
        count = (User.objects.filter(is_active=True,
                                     date_joined__lte=date).count())
        return {
            'count': count,
        }


class NewUsers(BaseMetricDocType):

    class Meta(object):
        mapping = Mapping('new_users')
        mapping.meta('_all', enabled=False)

    @classmethod
    def fetch_data(cls, date):
        start = date
        end = date + datetime.timedelta(days=1)
        count = (User.objects.filter(is_active=True,
                                     date_joined__range=(start, end)).count())
        return {
            'count': count,
        }


class NewBans(BaseMetricDocType):

    class Meta(object):
        mapping = Mapping('new_bans')
        mapping.meta('_all', enabled=False)

    @classmethod
    def fetch_data(cls, date):
        start = date
        end = date + datetime.timedelta(days=1)
        count = UserBan.objects.filter(is_active=True,
                                       date__range=(start, end)).count()
        return {
            'count': count,
        }


class NewFirstEditors(BaseMetricDocType):

    class Meta(object):
        mapping = Mapping('first_editors')
        mapping.meta('_all', enabled=False)

    @classmethod
    def fetch_data(cls, date):
        first_editors = []
        start = date
        end = date + datetime.timedelta(days=1)
        day_editors = (Revision.objects
                           .filter(created__range=(start, end))
                           .only('creator')
                           .select_related('creator')
                           .values_list('creator', flat=True)
                           .distinct())
        for pk in day_editors:
            oldest_edit = Revision.objects.filter(creator__pk=pk).aggregate(Min('created'))
            if oldest_edit['created__min'] >= start:
                first_editors.append(pk)
        count = len(first_editors)
        return {
            'count': count,
        }
# TODO:
# no. of contributors (from MDN) (anyone who has authored a revision in last 30 days)
# no. contributions (number of overall revisions)
