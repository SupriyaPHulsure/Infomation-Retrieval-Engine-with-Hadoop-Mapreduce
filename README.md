# Simple-Infomation-Retrieval-Engine-with-Hadoop-Mapreduce

In this project, a simple search engine is implemented using MapReduce framework on Hadoop. 

MapReduce is a programming model for processing and generating big data sets with parallel, distributed computing on cluster. Hadoop is an open source implementation of MapReduce model of programming. The implementation consists of two modules as follows-

 ###    1. Indexing of Documents -  
 
Master-index is created for input documents and all keywords in these documents are created using this program. Input is the directory path of documents. Master-index consists of a posting list for every term(word) with data as the filename and frequency of that keyword in that file in the form of <Di, Fi>. Di is document id and Fi is frequency of that term in Document Di. 
This Master-index is used when user wants to search keywords in these documents and gets output as list of documents ranked by the frequency of that keyword in these documents. 
 
 ###    2. Search Keywords – 

One or more keywords are given as an input to this module and this module will find these keywords in Master-index table produced form Indexing module and return a ranked list of documents which contains these keywords. 

Map function reads the master-index file and searches for keywords. It emits the inverted index of those keywords to ranking module which sorts the documents in inverted index posting list of that keyword according to the frequency of occurrences of that term in that corresponding document. The reducer function emits (keyword, list(document ids)) pairs. This is the output of Search keywords module of this project.  


## Input File format
 
 any text files
	

## Copy Input File into HDFS
```	
hadoop fs -copyFromLocal $HOME/sampleInput.txt /sampleInput.txt
```

## Compiling the program into jar file 
To compile a MapReduce program, you need the hadoop library. (Use parameter –classpath, library
location: /usr/share/hadoop/hadoop-core-1.0.1.jar). After compiling all *.java files, the .class files
will be put into a dedicated folder, called class.

The next step is to compile your program, as follows:

```
bash-S1$> javac -verbose -classpath /opt/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar -d class src/*.java
```

The compilation outcome (.class files) is located in the directory class.
now to combine all *.class files into *.jar file - 

```
jar -cvf {Jar_filename}.jar -C class/  .
```

## OR

You can directly create jar file on your local (using eclipse) and copy it to hdfs like any other file and skip above step

## Execute the MapReduce Job
	
### to create Inverted index

format - 
```
hadoop jar {your jar file} {your main class name} [arg1] [arg2] ...
```

example - 

```
hadoop jar <pathForThisProject>/target/InvertedIndex.jar \
      miniGoogle.InvertedIndex  /sampleInputDirectory  /output/inverted {number of mapper tasks} {number of reducer tasks}
  ```
 ### Search Keywords
 ```
hadoop jar <pathForThisProject>/target/InvertedIndex.jar \
      miniGoogle.SearchKWC  /output/inverted  /output/search {number of mapper tasks} {number of reducer tasks} {Keyword1, keyword2, ...}
```
## Copy Output to Local File System  

### Inverted Index results 
```
  hadoop fs -get /output/inverted $HOME/output/inverted.txt
  ```
### Search keywords result
```
hadoop fs -get /output/search $HOME/output/search.txt
```
## Print out the results 

### Inverted Index
```
hadoop fs -cat /output/Inverted/*
```
### Search
```
hadoop fs -get /output/search $HOME/output/search.txt
```
	
## Output from Search 

Keyword1, keyword2 -> {(filename1, no_ocuurences_in_filename1), (filename2, no_occurences),...}

Person, love -> { (file1, 130), (file2, 85), (file3, 40) }
