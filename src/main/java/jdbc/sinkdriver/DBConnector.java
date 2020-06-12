package jdbc.sinkdriver;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ibm.db2.cmx.internal.json4j.JSONArray;
import com.ibm.db2.cmx.internal.json4j.JSONObject;
import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

public class DBConnector {

    Properties dbConfigProps = new Properties();
    IDatabase database = null;
    HashMap<String, String> sinkProps = null;
    Path configDirectory = null;

    /**
     * Class constructor to read all the required configuration to be used in db
     * helper methods
     * 
     * @param absolutePath - abs path to the configuration file to read all the
     *                     required parameters
     * @throws Exception
     */
    public DBConnector(String absolutePath) throws Exception {
        try {
            File fileConfig = new File(absolutePath);
            if (fileConfig.exists() == false) {
                throw new IOException("Failed to find DB configuration");
            }
            configDirectory = Paths.get(absolutePath).getParent();
            FileReader reader = new FileReader(fileConfig);
            dbConfigProps.load(reader);
            sinkProps = new HashMap<String, String>();
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL, dbConfigProps.getProperty("url"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER, dbConfigProps.getProperty("user"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD, dbConfigProps.getProperty("password"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE, dbConfigProps.getProperty("poolsize"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT, dbConfigProps.getProperty("schemaname"));

        } catch (Exception ex) {
            throw ex;
        }
    }

    /**
     * Establishes a connection with the datbase
     * 
     * @throws Exception
     */
    public void makeDatabase() throws Exception {
        DatabaseFactory databaseFactory = new DatabaseFactory();
        database = databaseFactory.makeDatabase(new JDBCSinkConfig(sinkProps));
        if (database == null) {
            throw new Exception("DB connection FAILED");
        } else {
            System.out.println("DB connection established.");
        }
    }

    /**
     * Inserts 'N' number of records as specifed in the formal parameter. The number
     * of records that are inserted will all be same.
     * 
     * @param numRecords
     * @throws Exception
     */
    public void insertRecords(int numRecords) throws Exception {
        ArrayList<SinkRecord> records = new ArrayList<SinkRecord>();
        try {
            //get the schema from the json input file
            Schema dateSchema = getSchema();
            org.apache.kafka.connect.data.Struct payload = new org.apache.kafka.connect.data.Struct(dateSchema);
            //build the payload from the json input file which must match the scheme above
            buildSamplePayload(payload);
            //create a single sink records 
            SinkRecord rec = new SinkRecord("topic", 1, null, null, dateSchema, payload, 0);
            for (int count = 0; count < numRecords; count++) {
                records.add(rec);
            }
            System.out.println("records: " + records.size());
            // insert records in database
            database.getWriter().insert(String.format("%s.%s", dbConfigProps.getProperty("schemaname").toUpperCase(),
                    dbConfigProps.getProperty("tablename").toUpperCase()), records);
            System.out.println(String.format("%d RECORDS PROCESSED", records.size()));
        } catch (Exception ex) {
            throw ex;
        }
    }

    /**
     * Helper method that returns the required schema
     * 
     * @return
     * @throws Exception
     */
    private Schema getSchema() throws Exception {
        String SCHEMA_FILE_NAME = "schema.json";
        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            String dataBuffer = new String(Files.readAllBytes(Paths.get(String.format("%s/%s", configDirectory, SCHEMA_FILE_NAME))));
            List<Map<String, Object>> schemaContents = JSONArray.parse(dataBuffer);

            for (Map<String, Object> element : schemaContents) {

                String valueType = element.get("type").toString().toLowerCase();
                String fieldName = element.get("name").toString();
                // Add other data types if needed
                switch (valueType) {
                    case "string":
                        builder.field(fieldName, Schema.STRING_SCHEMA);
                        break;
                    case "boolean":
                        builder.field(fieldName, Schema.BOOLEAN_SCHEMA);
                        break;
                    case "double":
                        builder.field(fieldName, Schema.FLOAT64_SCHEMA);
                        break;
                    default:
                        throw new Exception("Unrecognized data type found");
                }
            }
            builder.build();
        } catch (Exception ex) {
            throw ex;
        }
        return builder;
    }

    /**
     * Helper method that builds the payload in the provided 'struct'
     * 
     * @param payload
     * @throws Exception
     */
    private void buildSamplePayload(org.apache.kafka.connect.data.Struct payload) throws Exception {
        try {
            String PAYLOAD_DATA_FILE_NAME = "payload.json";
            String result = new String(Files.readAllBytes(Paths.get(String.format("%s/%s", configDirectory, PAYLOAD_DATA_FILE_NAME))));
            JSONObject payloadObject = JSONObject.parse(result);
            for (Object entry : payloadObject.entrySet()) {
                Entry<String, Object> element = (Entry<String, Object>) entry;
                payload.put(element.getKey().toString(), element.getValue());
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

}
