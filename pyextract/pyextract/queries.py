"""Module for building SQL queries."""

from typing import Iterable, Generator

class ParsedQuery(object):
    """Components of a SQL query that are parsed upon object creation."""
    # pylint: disable=too-few-public-methods,too-many-instance-attributes

    def __init__(self, query: str) -> None:
        """Parse a SQL query and save its parts.

        PROPERTIES:
            query: The original SQL query to parse.
            columns: List of columns in the table.
            limit: Maximum number of rows to extract.
            schema: Name of the schema to read from.
            table: Name of the table to read from.
            where: List of where statements to be fed into SAP.
        """
        self.query = query
        self._raw_parts = [item for item in self.query.split()]
        self._upper_parts = [item.upper() for item in self.query.split()]

        self.columns = None  # type: List[str]
        self.limit = None  # type: int
        self.schema = None  # type: str
        self.table = None  # type: str
        self.where = None  # type: List[str]

        self._parse_select_from()
        self._parse_where()

    def __repr__(self):
        return "<ParsedQuery: '%s'>" % self.query

    def _parse_select_from(self):
        """Parse the SELECT and FROM components of this query"""

        select_index = self._upper_parts.index('SELECT')
        from_index = self._upper_parts.index('FROM')

        # Parse out the optional TOP statement from the query
        row_limit_exists = self._upper_parts[select_index + 1] == 'TOP'
        if row_limit_exists:
            self.limit = int(self._raw_parts[select_index + 2])
            column_start_index = select_index + 3
        else:
            self.limit = None
            column_start_index = select_index + 1

        # Parse out the column names between the SELECT and FROM statements
        columns = self._raw_parts[column_start_index:from_index]
        columns = [col.split(',') for col in columns]
        columns = [col.strip() for col in flatten(columns)]
        columns = [col for col in columns if col]
        if columns != ['*']:
            self.columns = columns

        # If the columns have aliases, parse them out
        if self.columns:
            self.columns = [col.split('.')[-1] for col in self.columns]

        # Parse out the FROM statement to get table + schema + columns
        target = self._raw_parts[from_index + 1]
        target = target.replace('[', '').replace(']', '')
        split_parts = target.split('.')
        if len(split_parts) == 1:
            self.table = target
        else:
            self.schema = split_parts[-2]
            self.table = split_parts[-1]

    def _parse_where(self):
        """Parse the WHERE components of this query"""

        # Parse out optional WHERE conditions from the query
        try:
            where_index = self._upper_parts.index('WHERE')
        except ValueError:
            return

        where_statements = []
        for index, operator in enumerate(self._upper_parts[where_index+1:]):
            full_index = where_index + index + 1
            if operator in ('=', '<', '<=', '>', '>=', '<>'):
                statement = self._parse_where_comparison(full_index)
                where_statements += [statement]
            elif operator == 'IN':
                statement = self._parse_where_in(full_index)
                where_statements += [statement]

        self.where = where_statements

    def _parse_where_comparison(self, index):
        """Parse a one-to-one comparison from a WHERE clause."""

        _, first_item = _split_alias_from_item(self._raw_parts[index-1])
        operator = self._raw_parts[index]
        last_item = self._raw_parts[index+1]
        statement = '{} {} {}'.format(first_item, operator, last_item)

        start_quote = last_item[0]
        if start_quote in ("'", '"'):
            while last_item[-1] != start_quote:
                index += 1
                last_item = self._raw_parts[index+1]
                statement += ' ' + last_item

        return statement

    def _parse_where_in(self, index):
        """Parse a list of items to match a WHERE IN () clause."""

        _, first_item = _split_alias_from_item(self._raw_parts[index-1])
        last_item = self._raw_parts[index+1]
        statement = '{} IN {}'.format(first_item, last_item)

        while last_item[-1] != ')':
            index += 1
            last_item = self._raw_parts[index+1]
            statement += ' ' + last_item

        return statement


def _split_alias_from_item(item: str) -> tuple:
    """Parse an alias from an item and return both parts.

    If no alias exists for the item, returns None.
    """
    parts = item.split('.')
    target = parts[-1]

    if len(parts) == 2:
        alias = parts[0]
    else:
        alias = None

    return alias, target


def flatten(iterable: Iterable) -> Generator:
    """Flatten iterable to generator with depth of 1; leave strings intact."""
    for item in iterable:
        if hasattr(item, '__iter__') and not isinstance(item, str):
            for inner in flatten(item):
                yield inner
        else:
            yield item
