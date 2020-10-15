"""Abstract base class for all PyExtract Messengers."""

import abc

from ..utils import DataDefinition


class ABCMessenger(object):
    """Base object that handles queries and data over a database connection."""

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        """Instantiate base properties of a Messenger object.

        PROPERTIES:
            _conn (object): A generic Connection object from associated
                database python module (ceODBC, pyrfc, cx_Oracle, apsw).
        """
        self._conn = None

    def __repr__(self):
        """String representation of this object."""
        return "<Messenger={} using Connection={}>".format(type(self), self._conn)

    @abc.abstractmethod
    def get_metadata_from_query(self, query):
        """Return metadata about a query from the database."""
        raise NotImplementedError

    @abc.abstractmethod
    def begin_extraction(self, metadata: DataDefinition, chunk_size: int = None):
        """Begin pulling data from a query."""
        raise NotImplementedError

    @abc.abstractmethod
    def continue_extraction(self, chunk_size: int = None):
        """Continue pulling data from a query."""
        raise NotImplementedError

    @abc.abstractmethod
    def finish_extraction(self):
        """Clean up after a long-term extraction is finished"""
        raise NotImplementedError
