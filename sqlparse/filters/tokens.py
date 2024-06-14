#
# Copyright (C) 2009-2020 the sqlparse authors and contributors
# <see AUTHORS file>
#
# This module is part of python-sqlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

from sqlparse import tokens as T
from hashlib import sha1

def anonymize(string):
    if string.lower() in ['sum', 'if', 'last_value', 'trim', 'nullif', 'concat', 'cast', 'varchar', 'concat_ws', 'count', 'coalesce', 'min', 'max', 'lead', 'row_number', 'nulls', 'null', 'datediff', 'lag']:
        return string

    salt = "eaa35b02507a834edd0d219343fd4bd075f21762"
    # Identifiers can't start with a number, so we prepend 'i' to make
    # it always work. This is harmless for string constants as well.
    return "i" + sha1((salt + string).lower().encode('utf-8')).hexdigest()[:8]

class _CaseFilter:
    ttype = None

    def __init__(self, case=None):
        if case == 'hash':
            self.convert = anonymize
            return
        case = case or 'upper'
        self.convert = getattr(str, case)

    def process(self, stream):
        for ttype, value in stream:
            if ttype in self.ttype:
                value = self.convert(value)
            yield ttype, value


class KeywordCaseFilter(_CaseFilter):
    ttype = T.Keyword


class IdentifierCaseFilter(_CaseFilter):
    ttype = T.Name, T.String.Symbol

    def process(self, stream):
        for ttype, value in stream:
            if ttype in self.ttype and value.strip()[0] != '"':
                value = self.convert(value)
            yield ttype, value


class TruncateStringFilter:
    def __init__(self, width, char):
        self.width = width
        self.char = char

    def process(self, stream):
        for ttype, value in stream:
            if ttype != T.Literal.String.Single:
                yield ttype, value
                continue

            if value[:2] == "''":
                inner = value[2:-2]
                quote = "''"
            else:
                inner = value[1:-1]
                quote = "'"

            if len(inner) > self.width:
                value = ''.join((quote, inner[:self.width], self.char, quote))
            yield ttype, value

class HashStringFilter:
    def __init__(self):
        self.hasher = sha1

    def process(self, stream):
        for ttype, value in stream:
            if ttype != T.Literal.String.Single:
                yield ttype, value
                continue

            if value[:2] == "''":
                inner = value[2:-2]
                quote = "''"
            else:
                inner = value[1:-1]
                quote = "'"

            hashed_inner = anonymize(inner)
            value = ''.join((quote, hashed_inner, quote))
            yield ttype, value
