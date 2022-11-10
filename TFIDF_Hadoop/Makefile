DIR = tfidf
FILE = ./text.txt
INPUT = tfidf/input
OUTPUT1 = tfidf/output1
OUTPUT2 = tfidf/output2
OUTPUT3 = tfidf/output3

init:
	hdfs dfs -rm -r -f $(DIR)
	hadoop fs -mkdir $(DIR)
	hadoop fs -mkdir $(INPUT)
	hadoop fs -put $(FILE) $(INPUT)

tfidf: tfidf.jar
	hdfs dfs -rm -r -f $(OUTPUT1)
	hdfs dfs -rm -r -f $(OUTPUT2)
	hdfs dfs -rm -r -f $(OUTPUT3)
	hadoop jar $< $(basename $<) $(INPUT) $(OUTPUT1) $(OUTPUT2) $(OUTPUT3)

tfidf1: tfidf1.jar
	hdfs dfs -rm -r -f $(OUTPUT1)
	hadoop jar $< $(basename $<) $(INPUT) $(OUTPUT1)

tfidf2: tfidf2.jar
	hdfs dfs -rm -r -f $(OUTPUT2)
	hadoop jar $< $(basename $<) $(OUTPUT1) $(OUTPUT2)

tfidf3: tfidf3.jar
	hdfs dfs -rm -r -f $(OUTPUT3)
	hadoop jar $< $(basename $<) $(OUTPUT2) $(OUTPUT3)

%.jar: %.java
	hadoop com.sun.tools.javac.Main $<
	jar cf $@ $(basename $<)*.class

clean:
	rm -f *.class *.jar
	hdfs dfs -rm -r -f $(DIR)




