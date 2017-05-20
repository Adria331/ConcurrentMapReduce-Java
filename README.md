# ConcurrentMapReduce in Java
A concurrent version of the paradigm MapReduce for BigData.

Sintaxis: `ConcMapReduce <input dir> <ouput dir> [num_reducers]`

Default number of reducers = 2.

To compile:
`$ javac -cp guava-20.0-hal.jar:. *.java` 

To execute:
`$ java -cp guava-20.0-hal.jar:. WordCount ./Test/ ./Output/`



