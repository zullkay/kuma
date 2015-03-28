from datetime import timedelta

from django.contrib.auth.models import User
from django.utils import timezone

from kuma.users.models import UserBan


def active_users():
    return User.objects.filter(is_active=True)


def user_count():
    """
    Number of account holders (from MDN) (anyone who has an account at the time of viewing)
    """
    # we're excluding deactivated users here since that also applies to
    # banned users
    return active_users.count()


def new_users(days=30):
    """
    Number of new user accounts (default over last 30 days)
    """
    start = timezone.now() - timedelta(days=days)
    return active_users().filter(date_joined__gte=start).count()


def user_bans(days=30):
    """
    Number of user bans (default over the last 30 days)
    """
    start = timezone.now() - timedelta(days=days)
    return UserBan.objects.filter(is_active=True, date__gte=start).count()


# of first revisions (from MDN)(over last 30 days)

# contributors (from MDN)(over last 30 days?) (anyone who has authored a revision in last 30 days)

# contributions (number of overall revisions in last 30 days)
