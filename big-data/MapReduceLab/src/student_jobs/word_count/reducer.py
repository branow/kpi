from src.core.job.reducer import Reducer


class WordCountReducer(Reducer):
    def reduce(self, key, values, emit):
        emit(key, sum(values))


class WordVowelConsonantReducer(Reducer):
    def reduce(self, key, values, emit):
        vowel_pct = (sum(values) / len(values) / key) * 100
        cons_pct = 100 - vowel_pct
        emit(key, f"vowels {vowel_pct:.2f}% consonants {cons_pct:.2f}%")
