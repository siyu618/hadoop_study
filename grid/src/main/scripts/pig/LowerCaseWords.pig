%default MyUDF hadoop_study-core.jar

register $MyUDF;

define MyLower com.mycompany.hadoop_study.pig.eval.CommonLower;

LINE = load '$Input' using PigStorage() as (line:chararray);
LOWER_LINE = foreach LINE
    generate MyLower(line) as line;
store LOWER_LINE
    into '$Output'
    using PigStorage();
