import datetime

from elasticsearch_dsl import document, field
from elasticsearch_dsl.mapping import Mapping

from django.contrib.auth.models import User
from django.db.models import Count, Min

from kuma.users.models import UserBan, UserProfile
from kuma.wiki.models import Revision


INDEX = 'mdn-metrics'


class BaseMetricDocType(document.DocType):
    date = field.Date(format='date')
    count = field.Long()

    @classmethod
    def build_document(cls, date):
        """
        Implement this in your subclass.

        It expects a generator to be returned. The generator contents should be
        a dictionary with at least a 'count' and 'date' field in ISO format.

        For example, the most simple case of a single day's aggregate data::

            count = count_some_things()
            yield {
                'date': date.isoformat(),
                'count': count,
            }

        By requiring a generator this allows the data structure to be complex,
        even so far as returning all items with count=1 if need be.

        Any extra fields in this dictionary will be pushed to Elasticsearch.
        These extra fields could be used for filtering data further or other
        aggregations.
        """
        raise NotImplementedError(cls)

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
    """
    Count all users on a given day, per locale.
    """

    class Meta(object):
        mapping = Mapping('total_users')
        mapping.meta('_all', enabled=False)

    @classmethod
    def build_document(cls, date):
        counts = (
            UserProfile.objects.filter(user__is_active=True,
                                       user__date_joined__lte=date)
                               .values_list('locale')
                               .annotate(locales=Count('locale'))).iterator()
        for count in counts:
            yield {
                'date': date.isoformat(),
                'count': count[1],
                'locale': count[0],
            }


class NewUsers(BaseMetricDocType):
    """
    Count number of new users per day.
    """

    class Meta(object):
        mapping = Mapping('new_users')
        mapping.meta('_all', enabled=False)

    @classmethod
    def build_document(cls, date):
        start = date
        end = date + datetime.timedelta(days=1)
        count = (User.objects.filter(is_active=True,
                                     date_joined__range=(start, end)).count())
        yield {
            'date': date.isoformat(),
            'count': count,
        }


class NewBans(BaseMetricDocType):
    """
    Count number of new bans per day.
    """

    class Meta(object):
        mapping = Mapping('new_bans')
        mapping.meta('_all', enabled=False)

    @classmethod
    def build_document(cls, date):
        start = date
        end = date + datetime.timedelta(days=1)
        count = UserBan.objects.filter(is_active=True,
                                       date__range=(start, end)).count()
        yield {
            'date': date.isoformat(),
            'count': count,
        }


class NewFirstEditors(BaseMetricDocType):
    """
    Count users who have contributed their first edit on this date.
    """

    class Meta(object):
        mapping = Mapping('first_editors')
        mapping.meta('_all', enabled=False)

    @classmethod
    def build_document(cls, date):
        first_editors = []
        start = date
        end = date + datetime.timedelta(days=1)
        day_editors = (Revision.objects.filter(created__range=(start, end))
                                       .only('creator')
                                       .select_related('creator')
                                       .values_list('creator', flat=True)
                                       .distinct())
        for pk in day_editors:
            oldest_edit = (Revision.objects.filter(creator__pk=pk)
                                           .aggregate(Min('created')))
            if oldest_edit['created__min'] >= start:
                first_editors.append(pk)
        count = len(first_editors)
        yield {
            'date': date.isoformat(),
            'count': count,
        }

# TODO:
# no. of contributors (from MDN) (anyone who has authored a revision in last 30 days)
# no. contributions (number of overall revisions)
