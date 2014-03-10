hadoop_study
============

<<<<<<< HEAD
Version : 1.0.0
Date : 2014.03.07
   * Add MR UT and Pig UT

MR UT + Pig UT
MR UT
1. mockito
  http://docs.mockito.googlecode.com/hg/org/mockito/Mockito.html 
2. MR Unit
  JUnit (Testng got error)
https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial 
Pig UT
1. How do we test pig manually 
  Grunt shell : describe, illustrate, dump
  pig -x local 
  pig [-x mapreduce]
  Pig UT
2. Test it automatically
Pigunit
  Testng and Junit are both supported.
  Override variables in the pig script to get input from test code.
  Drops STORE and DUMP .
  Inspect the values in the script.
  Like the cmds in Pig Grunt

Pig UT
3. Integrate with maven
  One input (simple case)
  Multiple Inputs (not supported natively)
  UDF test
    Test for UDF class
    Test script with UDF
      UDF in other package/jar
        dependency
      UDF in the same package/jar
        Build jar before run the test



=======
hadoop related examples
>>>>>>> 76211a9c33284ad1715c03107f452ad9a51a251c
