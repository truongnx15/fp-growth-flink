# Parallel FPGrowth in Apache Flink
The implementation of [FPGrowth](http://dl.acm.org/citation.cfm?id=335372) and [Parallel FPGrowth](http://dl.acm.org/citation.cfm?id=1454027) for mining all frequent itemsets. The local FPGrowth is a typical scala program. The parallel FPGrowth is implemented in Apache Flink. The program read database of transactions from an input file. Each transaction is on one line and items within the transaction are non-empty string(no spaces) seperated by space delimiter.

======

# Requirements
* Maven version 3.2.2 or newer
* HADOOP_HOME is set

# Build
* `mvn clean package` to build and run unit tests before building
* `mvn clean package -DskipTests` to build and ignore unit tests

# Parameters
* --input: Path to the input file
* --support: min support
* --group: number of groups to which items should be divided. The default value is number of parallelisms of Apache Flink. This parameter is not applicable for Local FPGrowth version

# Example
For more details, please look at package example to check how to run and mine all frequent itemsets
* **Local version:** `scala 
    -Xmx2g 
    -cp target/parallel-fpgrowth-1.0-SNAPSHOT.jar example.LocalFPGrowth 
    --input sample-input.txt 
    --support 0.2`
* **Parallel version:** `$FLINK_HOME/bin/flink run -class example.FlinkFPGrowth target/parallel-fpgrowth-1.0-SNAPSHOT.jar 
    --input sample-input.txt 
		--support 0.2 
		--group 100`  




