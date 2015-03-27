import datetime
from django.conf import settings
import googleanalytics as ga


def user_count(days=1):
    """
    Fetch the number of users on MDN for the given number of days
    """
    accounts = ga.authenticate(**settings.GOOGLE_ANALYTICS_CREDENTIALS)
    account = accounts[0]

    webproperty = account.webproperties[0]
    profile = webproperty.profiles[0]

    now = datetime.datetime.now()
    yesterday = (now - datetime.timedelta(days=days)).date()

    report = profile.core.query('users').range(yesterday.isoformat()).get()
    return report['users'][0]
