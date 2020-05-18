import logging
import attr
import boto3

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class SessionSource:
    """
    Trivial base class for creating sessions that assumes account number
    is the profile name.

    If cross-account hopping (via STS assume-role) is necessary,
    subclass this and implement your own session creation logic.

    """

    def get_session(self, account_number: str) -> boto3.Session:
        return boto3.Session(profile_name=account_number)
