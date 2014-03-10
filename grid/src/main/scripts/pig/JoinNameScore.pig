--function: join the student table and score table by id;
--no matched will be ignored.


ID_NAME = load '$InputName' using PigStorage('|') as (id:chararray, name:chararray);
ID_SCORE = load '$InputScore' using PigStorage('|') as (id:chararray, score:int);

NAME_SCORE = join ID_NAME by id, ID_SCORE by id;

ID_NAME_SCORE = foreach NAME_SCORE
    generate
    ID_NAME::id as id,
    ID_NAME::name as name,
    ID_SCORE::score as score;

ID_NAME_SCORE_ORDERED = order ID_NAME_SCORE by score asc;
store ID_NAME_SCORE_ORDERED
    into '$Output'
    using PigStorage('|');
