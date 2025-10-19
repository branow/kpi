# MapReduce Python Simulator

## Quickstart

* Create a virtual environment and install the requirements if needed.
* Run a sample job after scaffolding is complete:

### Count Words

```bash
python -m src.cli.main run \
  --workers 4 \
  --input data/input \
  --output data/output/wordcount \
  --job student_jobs.word_count.mapper:WordCountMapper,student_jobs.word_count.reducer:WordCountReducer \
  --reducers 4
```

### Count Words Longer Than 5 Characters

```bash
python -m src.cli.main run \
  --workers 4 \
  --input data/input \
  --output data/output/longwordcount \
  --job student_jobs.word_count.mapper:LongWordCountMapper,student_jobs.word_count.reducer:WordCountReducer \
  --reducers 4
```

### Count Percentage of Vowels and Consonants by Word Length

```bash
python -m src.cli.main run \
  --workers 4 \
  --input data/input \
  --output data/output/vowel_consonant_stats \
  --job student_jobs.word_count.mapper:WordVowelConsonantMapper,student_jobs.word_count.reducer:WordVowelConsonantReducer \
  --reducers 4
```
