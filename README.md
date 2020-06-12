# JDBC-SINKDRIVER

### This sample driver works is meant to be used with IBM-MESSAGING/KAFKA-CONNECT-JDBC-SINK to test its code again SQL DBs such as DB2, MYSQL, POSTGRES, etc..

### General Instructions
1.  Download and build the code from <https://github.com/ibm-messaging/kafka-connect-jdbc-sink.git>.
1.  Get the JAR file from the step above and make it available via a public / private / local maven repository.  If the jar file is already available in a repo then the 1st step is not required.  (Optional) Add the JAR file to your local maven repository, see example below:
    ```
    mvn install:install-file -Dfile=<path_to_jar_/kafka-connect-jdbc-sink-0.0.1-SNAPSHOT-jar-with-dependencies.jar> -DgroupId=com.ibm.eventstreams.connect -DartifactId=kafka-connect-jdbc-sink -Dversion=<upate-version-0.0.1-SNAPSHOT>
    ```

1.  If required update the POM.xml file so that it has the dependency listed as below:
    ```
     <dependency>
        <groupId>com.ibm.eventstreams.connect</groupId>
        <artifactId>kafka-connect-jdbc-sink</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
    ```
1.  Make sure that the `config` directory is located in `src/config` and it has the following items:
    
    1.  db.config.properties - configuration settings
    1.  schema.json - describes the data schema 
    1.  payload.json - sample data that matches the schema

## Debugging in an IDE
1.  To debug in VSCODE or any other IDE add the approprite debug settings to project / environment
    1.  Start debugging the main() method in `Driver.java` class located in `src/main/java/sinkdriver/` directory.
    2.  `Driver.java` will call the `DBConnector` class to read the cofiguration, create a database connection and insert data into the specifid database tables.


## Build and execute from command line / terminal
1.  Open a terminal and run the command `mvn clean package` to build the project:
1.  To execute use the terminal to execute the following:
    ```
    java -jar <path_to_jar>.jar
    ```
1.  The driver also accepts 2 `optional` parameters:
    1.  `-rc` - the record count to insert.  Default: `10` 
    1.  `-cp` - the configuration file absolute path.  Default: `/.../src/config`
