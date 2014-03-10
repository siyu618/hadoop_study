-- script to do the wordcount stuff
-- input: text lines
-- output: word|count

RAW_LINE = load '$Input' using PigStorage() as (line:chararray);

WORD_LIST = foreach RAW_LINE generate flatten(TOKENIZE((line))) as word:chararray;

WORD_GROUP = group WORD_LIST by word;

WORD_COUNT = foreach WORD_GROUP generate group as word, COUNT(WORD_LIST) as count;
WORD_COUNT_A = order WORD_COUNT by word asc;
store WORD_COUNT_A
    into '$Output'
    using PigStorage('|');
