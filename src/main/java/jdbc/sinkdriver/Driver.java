package jdbc.sinkdriver;

import java.io.File;
import org.apache.commons.cli.*;

/**
 * Driver class that uses the helper class `DBConnector` to insert data in DB2 (and others)
 */
public final class Driver {

    public static void main(String[] args) {
        System.out.println("Begin Execution");
        DBConnector dbConn = null;
        try {
       
            // read the required cmd values
            Options options = new Options();
            Option recCount = new Option("rc", "recordcount", true, "number of records");
            recCount.setRequired(false);
            options.addOption(recCount);
            Option configPath = new Option("cp", "configpath", true, "config file absolute path");
            configPath.setRequired(false);
            options.addOption(configPath);
            
            CommandLine cmd = new DefaultParser().parse(options, args);
            String absConfigPath = cmd.getOptionValue("cp", String.format("%s/%s", new File("").getAbsolutePath(), "src/config/db.config.properties"));
            int recordCount = Integer.parseInt(cmd.getOptionValue("rc", "1"));
      
            System.out.println(String.format("Inserting '%d' record(s) while using configuration path '%s'", recordCount, absConfigPath));

            //call the database
            dbConn = new DBConnector(absConfigPath);
            dbConn.makeDatabase();
            dbConn.insertRecords(recordCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("End Execution");
    }
}

