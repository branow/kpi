import re
from src.core.job.mapper import Mapper

VOWELS = (
    "aeiou" +
    "àâæçéèêëîïôœùûüÿ" +
    "àèéìòù" +
    "áéíóúü" +
    "äöü" +
    "ąęó" +
    "аеєиіїоуюя"
)


def tokenize(record: str):
    """
    Convert record to lowercase, replace underscores with spaces,
    extract alphabetic word tokens, and return list of words.
    """
    record = str(record).lower().replace("_", " ")
    tokens = re.findall(r'\b\w+\b', record)
    return [t for t in tokens if all(c.isalpha() for c in t)]


class WordCountMapper(Mapper):
    def map(self, record, emit):
        for token in tokenize(record):
            emit(token, 1)


class LongWordCountMapper(Mapper):
    def map(self, record, emit):
        for token in tokenize(record):
            if len(token) > 5:
                emit(token, 1)


class WordVowelConsonantMapper(Mapper):
    def map(self, record, emit):
        for token in tokenize(record):
            vowels_count = sum(1 for c in token if c in VOWELS)
            emit(len(token), vowels_count)
