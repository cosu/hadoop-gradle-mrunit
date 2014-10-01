Template project for hadoop v2 apps built with Gradle and tested with MRunit


It includes two hadoop applications. One is wordcount and the other one is the sms code count from teh MRunit website.

```gradle jar``` generates the hadoop-ready jar file in build/lib.

To execute the wordcount example first copy the file to the hdfs store:

    hdfs dfs -cp test1.txt .

execute with

    hadoop jar hadoop-bootstrap-1.0.jar ro.cosu.wordcount.WordCount test.txt ./out1.txt


In test/src you can find examples on how to test the mappers and the reducers for the two applications.