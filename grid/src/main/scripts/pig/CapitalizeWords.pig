%default MyUDF hadoop_study-grid.jar

register $MyUDF;

define MyUPPER com.mycompany.hadoop_study.grid.pig.eval.MyUPPER;

LINE = load '$Input' using PigStorage() as (line:chararray);
UPPER_LINE = foreach LINE
    generate MyUPPER(line) as line;
store UPPER_LINE
    into '$Output'
    using PigStorage();
