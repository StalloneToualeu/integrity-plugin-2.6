/*******************************************************************************
 * Contributors: PTC 
 *******************************************************************************/
package hudson.scm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.ConnectionPoolDataSource;

import org.apache.commons.io.IOUtils;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;

import com.mks.api.response.APIException;
import com.mks.api.response.Field;
import com.mks.api.response.WorkItem;
import com.mks.api.response.WorkItemIterator;
import com.mks.api.si.SIModelTypeName;

import hudson.model.TaskListener;
import hudson.scm.IntegritySCM.DescriptorImpl;
import hudson.scm.api.option.IAPIFields;

/**
 * This class provides certain utility functions for working with the embedded
 * derby database
 */
public class DerbyUtils {

    private DerbyUtils() {
        throw new IllegalStateException("Utility class");
    }

    private static final Logger LOGGER = Logger.getLogger("IntegritySCM");
    public static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    public static final String DERBY_SYS_HOME_PROPERTY = "derby.system.home";
    public static final String DERBY_URL_PREFIX = "jdbc:derby:";
    private static final String DERBY_DB_NAME = "IntegritySCM";
    public static final String CREATE_INTEGRITY_SCM_REGISTRY = "CREATE TABLE INTEGRITY_SCM_REGISTRY ("
            + "ID INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
            + "JOB_NAME VARCHAR(256) NOT NULL, "
            + "CONFIGURATION_NAME VARCHAR(50) NOT NULL, "
            + "PROJECT_CACHE_TABLE VARCHAR(50) NOT NULL, "
            + "BUILD_NUMBER BIGINT NOT NULL)";
    public static final String SELECT_REGISTRY_1 = "SELECT ID FROM INTEGRITY_SCM_REGISTRY WHERE ID = 1";
    public static final String SELECT_REGISTRY_TABLE = "SELECT PROJECT_CACHE_TABLE FROM INTEGRITY_SCM_REGISTRY WHERE JOB_NAME = ? AND CONFIGURATION_NAME = ? AND BUILD_NUMBER = ?";
    public static final String SELECT_REGISTRY_TABLE_DROP = "SELECT PROJECT_CACHE_TABLE FROM INTEGRITY_SCM_REGISTRY WHERE JOB_NAME = ? AND BUILD_NUMBER = ?";
    public static final String INSERT_REGISTRY_ENTRY = "INSERT INTO INTEGRITY_SCM_REGISTRY (JOB_NAME, CONFIGURATION_NAME, PROJECT_CACHE_TABLE, BUILD_NUMBER) " + "VALUES (?, ?, ?, ?)";
    public static final String SELECT_REGISTRY_DISTINCT_PROJECTS = "SELECT DISTINCT JOB_NAME FROM INTEGRITY_SCM_REGISTRY";
    public static final String SELECT_REGISTRY_PROJECTS = "SELECT PROJECT_CACHE_TABLE FROM INTEGRITY_SCM_REGISTRY WHERE JOB_NAME = ? AND CONFIGURATION_NAME = ? ORDER BY BUILD_NUMBER DESC";
    public static final String SELECT_REGISTRY_PROJECT = "SELECT PROJECT_CACHE_TABLE FROM INTEGRITY_SCM_REGISTRY WHERE JOB_NAME = ?";
    public static final String DROP_REGISTRY_ENTRY = "DELETE FROM INTEGRITY_SCM_REGISTRY WHERE PROJECT_CACHE_TABLE = ?";
    public static final String CREATE_PROJECT_TABLE = "CREATE TABLE CM_PROJECT ("
            + CM_PROJECT.ID + " INTEGER NOT NULL "
            + "PRIMARY KEY GENERATED ALWAYS AS IDENTITY "
            + "(START WITH 1, INCREMENT BY 1), "
            + CM_PROJECT.TYPE + " SMALLINT NOT NULL, "
            + /* 0 = File; 1 = Directory */ CM_PROJECT.NAME + " VARCHAR(32500) NOT NULL, "
            + CM_PROJECT.MEMBER_ID + " VARCHAR(32500), "
            + CM_PROJECT.TIMESTAMP + " TIMESTAMP, "
            + CM_PROJECT.DESCRIPTION + " CLOB(4 M), "
            + CM_PROJECT.AUTHOR + " VARCHAR(100), "
            + CM_PROJECT.CONFIG_PATH + " VARCHAR(32500), "
            + CM_PROJECT.REVISION + " VARCHAR(32500), "
            + CM_PROJECT.OLD_REVISION + " VARCHAR(32500), "
            + CM_PROJECT.RELATIVE_FILE + " VARCHAR(32500), "
            + CM_PROJECT.CHECKSUM + " VARCHAR(32), "
            + CM_PROJECT.DELTA + " SMALLINT)";
    /* 0 = Unchanged; 1 = Added; 2 = Changed; 3 = Dropped */
    public static final String DROP_PROJECT_TABLE = "DROP TABLE CM_PROJECT";
    public static final String SELECT_MEMBER_1 = "SELECT " + CM_PROJECT.ID + " FROM CM_PROJECT WHERE " + CM_PROJECT.ID + " = 1";
    
    /** ------------ CP Cache tables -------------------- **/
    public static final String SELECT_CP_1 =
        "SELECT " + CM_PROJECT.ID + " FROM CM_PROJECT_CP WHERE " + CM_PROJECT.ID + " = 1";
    public static final String CREATE_PROJECT_CP_TABLE = "CREATE TABLE CM_PROJECT_CP ("
        + CM_PROJECT.ID + " INTEGER NOT NULL " + "PRIMARY KEY GENERATED ALWAYS AS IDENTITY "
        + "(START WITH 1, INCREMENT BY 1), " + CM_PROJECT.CPID + " VARCHAR(32500) NOT NULL, "
        + CM_PROJECT.CP_STATE + " VARCHAR(32500) NOT NULL)";
    public static final String INSERT_CP_RECORD = "INSERT INTO CM_PROJECT_CP " + "(" + CM_PROJECT.CPID
        + ", " + CM_PROJECT.CP_STATE + ") " + "VALUES (?, ?)";
    public static final String CP_SELECT =
        "SELECT " + CM_PROJECT.CPID + ", " + CM_PROJECT.CP_STATE + " FROM CM_PROJECT_CP";
    public static final String DELETE_CP_RECORD =
        "DELETE FROM CM_PROJECT_CP WHERE " + CM_PROJECT.CPID + " = ?";
    /** ------------ CP Cache tables end -------------------- **/
    
    
    public static final String INSERT_MEMBER_RECORD = "INSERT INTO CM_PROJECT "
            + "(" + CM_PROJECT.TYPE + ", " + CM_PROJECT.NAME + ", " + CM_PROJECT.MEMBER_ID + ", "
            + CM_PROJECT.TIMESTAMP + ", " + CM_PROJECT.DESCRIPTION + ", " + CM_PROJECT.CONFIG_PATH + ", "
            + CM_PROJECT.REVISION + ", " + CM_PROJECT.RELATIVE_FILE + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String BASELINE_SELECT = "SELECT " + CM_PROJECT.NAME + ", " + CM_PROJECT.MEMBER_ID + ", " + CM_PROJECT.TIMESTAMP + ", "
            + CM_PROJECT.DESCRIPTION + ", " + CM_PROJECT.AUTHOR + ", " + CM_PROJECT.CONFIG_PATH + ", "
            + CM_PROJECT.REVISION + ", " + CM_PROJECT.RELATIVE_FILE + ", " + CM_PROJECT.CHECKSUM
            + " FROM CM_PROJECT WHERE " + CM_PROJECT.TYPE + " = 0 AND ("
            + CM_PROJECT.DELTA + " IS NULL OR " + CM_PROJECT.DELTA + " <> 3)";
    public static final String DELTA_SELECT = "SELECT " + CM_PROJECT.TYPE + ", " + CM_PROJECT.NAME + ", " + CM_PROJECT.MEMBER_ID + ", "
            + CM_PROJECT.TIMESTAMP + ", " + CM_PROJECT.DESCRIPTION + ", " + CM_PROJECT.AUTHOR + ", "
            + CM_PROJECT.CONFIG_PATH + ", " + CM_PROJECT.REVISION + ", " + CM_PROJECT.OLD_REVISION + ", "
            + CM_PROJECT.RELATIVE_FILE + ", " + CM_PROJECT.CHECKSUM + ", " + CM_PROJECT.DELTA
            + " FROM CM_PROJECT WHERE " + CM_PROJECT.TYPE + " = 0";
    public static final String PROJECT_SELECT = "SELECT " + CM_PROJECT.NAME + ", " + CM_PROJECT.MEMBER_ID + ", " + CM_PROJECT.TIMESTAMP + ", "
            + CM_PROJECT.DESCRIPTION + ", " + CM_PROJECT.AUTHOR + ", " + CM_PROJECT.CONFIG_PATH + ", "
            + CM_PROJECT.REVISION + ", " + CM_PROJECT.OLD_REVISION + ", " + CM_PROJECT.RELATIVE_FILE + ", "
            + CM_PROJECT.CHECKSUM + ", " + CM_PROJECT.DELTA
            + " FROM CM_PROJECT WHERE " + CM_PROJECT.TYPE + " = 0 ORDER BY " + CM_PROJECT.NAME + " ASC";

    public static final String SUB_PROJECT_SELECT = "SELECT " + CM_PROJECT.NAME + ", " + CM_PROJECT.CONFIG_PATH + ", " + CM_PROJECT.REVISION
            + " FROM CM_PROJECT WHERE " + CM_PROJECT.TYPE + " = 1 ORDER BY " + CM_PROJECT.CONFIG_PATH + " ASC";

    public static final String AUTHOR_SELECT = "SELECT " + CM_PROJECT.NAME + ", " + CM_PROJECT.MEMBER_ID + ", " + CM_PROJECT.AUTHOR + ", "
            + CM_PROJECT.CONFIG_PATH + ", " + CM_PROJECT.REVISION + " FROM CM_PROJECT WHERE "
            + CM_PROJECT.TYPE + " = 0 AND (" + CM_PROJECT.DELTA + " IS NULL OR " + CM_PROJECT.DELTA + " <> 3)";

    // accordingly to AUTHOR_SELECT
    public static final String AUTHOR_SELECT_COUNT = "SELECT COUNT(*) FROM CM_PROJECT WHERE " + CM_PROJECT.TYPE + " = 0 AND (" + CM_PROJECT.DELTA + " IS NULL OR " + CM_PROJECT.DELTA + " <> 3)";

    public static final String DIR_SELECT = "SELECT DISTINCT " + CM_PROJECT.RELATIVE_FILE + " FROM CM_PROJECT WHERE "
            + CM_PROJECT.TYPE + " = 1 ORDER BY " + CM_PROJECT.RELATIVE_FILE + " ASC";
    public static final String CHECKSUM_UPDATE = "SELECT " + CM_PROJECT.NAME + ", " + CM_PROJECT.CHECKSUM + " FROM CM_PROJECT WHERE "
            + CM_PROJECT.TYPE + " = 0 AND (" + CM_PROJECT.DELTA + " IS NULL OR " + CM_PROJECT.DELTA + " <> 3)";

    /**
     * Returns the CM_PROJECT column name for the string column name
     *
     * @param name
     * @return
     */
    public static final CM_PROJECT getEnum(String name) {
        CM_PROJECT[] values = CM_PROJECT.values();
        for (CM_PROJECT value : values) {
            if (name.equals(value.toString())) {
                return value;
            }
        }
        return CM_PROJECT.UNDEFINED;
    }

    /**
     * Random unique id generator for cache table names
     *
     * @return
     */
    public static final String getUUIDTableName() {
        return "SCM_" + UUID.randomUUID().toString().replace('-', '_');
    }

    /**
     * Utility function to load the Java DB Driver
     */
    public static void loadDerbyDriver() {
        try {
            LOGGER.fine("Loading derby driver: " + DERBY_DRIVER);
            Class.forName(DERBY_DRIVER);
        } catch (ClassNotFoundException ex) {
            LOGGER.severe("Failed to load derby driver: " + DERBY_DRIVER);
            LOGGER.severe(ex.getMessage());
            LOGGER.log(Level.SEVERE, "ClassNotFoundException", ex);
        }
    }

    /**
     * Creates a pooled connection data source for the derby database
     *
     * @param derbyHome
     * @return
     */
    public static ConnectionPoolDataSource createConnectionPoolDataSource(String derbyHome) {
        EmbeddedConnectionPoolDataSource dataSource = new EmbeddedConnectionPoolDataSource();
        dataSource.setCreateDatabase("create");
        dataSource.setDataSourceName(DERBY_URL_PREFIX + derbyHome.replace('\\', '/') + "/" + DERBY_DB_NAME);
        dataSource.setDatabaseName(derbyHome.replace('\\', '/') + "/" + DERBY_DB_NAME);

        return dataSource;

    }

    /**
     * Generic SQL statement execution function
     *
     * @param dataSource A pooled connection data source
     * @param sql String sql statement
     * @return
     * @throws SQLException
     */
    public static synchronized boolean executeStmt(ConnectionPoolDataSource dataSource, String sql) throws SQLException {
        boolean success = false;
        Connection db = null;
        Statement stmt = null;
        try {
            LOGGER.log(Level.FINE, "Preparing to execute {0}", sql);
            db = dataSource.getPooledConnection().getConnection();
            stmt = db.createStatement();
            success = stmt.execute(sql);
            LOGGER.fine("Executed...!");
        } finally {
            if (null != stmt) {
                stmt.close();
            }

            if (null != db) {
                db.close();
            }
        }

        return success;
    }

    /**
     * Creates the Integrity SCM cache registry table
     *
     * @param dataSource
     * @return
     */
    public static synchronized boolean createRegistry(ConnectionPoolDataSource dataSource) {
        boolean tableCreated = false;
        try {
            if (executeStmt(dataSource, SELECT_REGISTRY_1)) {
                LOGGER.fine("Integrity SCM cache registry table exists...");
                tableCreated = true;
            }
        } catch (SQLException ex) {
            LOGGER.fine(ex.getMessage());
            try {
                LOGGER.fine("Integrity SCM cache registry doesn't exist, creating...");
                tableCreated = executeStmt(dataSource, CREATE_INTEGRITY_SCM_REGISTRY);
            } catch (SQLException sqlex) {
                LOGGER.fine("Failed to create Integrity SCM cache registry table!");
                LOGGER.log(Level.SEVERE, "SQLException", sqlex);
                tableCreated = false;
            }
        }

        return tableCreated;
    }

    /**
     * Creates a single Integrity SCM Project/Configuration cache table
     *
     * @param dataSource
     * @param jobName
     * @param configurationName
     * @param buildNumber
     * @return
     * @throws SQLException
     */
    public static synchronized String registerProjectCache(ConnectionPoolDataSource dataSource, String jobName, String configurationName, long buildNumber) throws SQLException {
        String cacheTableName = "";
        Connection db = null;
        PreparedStatement insert = null;

        try {
            // First Check to see if the current project registry exists
            db = dataSource.getPooledConnection().getConnection();
            cacheTableName = getProjectCache(dataSource, jobName, configurationName, buildNumber);
            if (null == cacheTableName || cacheTableName.length() == 0) {
                // Insert a new row in the registry
                String uuid = getUUIDTableName();
                insert = db.prepareStatement(INSERT_REGISTRY_ENTRY);
                insert.clearParameters();
                insert.setString(1, jobName);			// JOB_NAME
                insert.setString(2, configurationName);	// CONFIGURATION_NAME
                insert.setString(3, uuid);				// PROJECT_CACHE_TABLE
                insert.setLong(4, buildNumber);			// BUILD_NUMBER
                insert.executeUpdate();
                cacheTableName = uuid;
            }
        } catch (SQLException sqlex) {
            LOGGER.fine(String.format("Failed to create Integrity SCM cache registry entry for %s/%s/%d!", jobName, configurationName, buildNumber));
            LOGGER.log(Level.SEVERE, "SQLException", sqlex);
        } finally {
            if (null != insert) {
                insert.close();
            }
            if (null != db) {
                db.close();
            }
        }

        return cacheTableName;
    }

    /**
     * Returns the name of the project cache table for the specified
     * job/configuration and build
     *
     * @param dataSource
     * @param jobName
     * @param configurationName
     * @param buildNumber
     * @return
     * @throws SQLException
     */
    public static synchronized String getProjectCache(ConnectionPoolDataSource dataSource, String jobName, String configurationName, long buildNumber) throws SQLException {
        String cacheTableName = "";
        Connection db = null;
        PreparedStatement select = null;
        ResultSet rs = null;

        try {
            db = dataSource.getPooledConnection().getConnection();
            select = db.prepareStatement(SELECT_REGISTRY_TABLE, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            select.setString(1, jobName);
            select.setString(2, configurationName);
            select.setLong(3, buildNumber);
            rs = select.executeQuery();
            if (getRowCount(rs) > 0) {
                rs.next();
                cacheTableName = rs.getString("PROJECT_CACHE_TABLE");
            }
        } catch (SQLException sqlex) {
            LOGGER.fine(String.format("Failed to get Integrity SCM cache registry entry for %s/%s/%d!", jobName, configurationName, buildNumber));
            LOGGER.log(Level.SEVERE, "SQLException", sqlex);
        } finally {
            if (null != select) {
                select.close();
            }
            if (null != rs) {
                rs.close();
            }
            if (null != db) {
                db.close();
            }
        }

        return cacheTableName;
    }

    /**
     * Maintenance function that returns a list of distinct job names for
     * additional checking to see which ones are inactive
     *
     * @param dataSource
     * @return
     * @throws SQLException
     */
    public static synchronized List<String> getDistinctJobNames(ConnectionPoolDataSource dataSource) throws SQLException {
        List<String> jobsList = new ArrayList<>();

        // First Check to see if the current project registry exists
        LOGGER.fine("Preparing to execute " + SELECT_REGISTRY_DISTINCT_PROJECTS);
        try (
                Connection db = dataSource.getPooledConnection().getConnection();
                PreparedStatement select = db.prepareStatement(SELECT_REGISTRY_DISTINCT_PROJECTS);
                ResultSet rs = select.executeQuery();) {

            LOGGER.fine("Executed!");
            while (rs.next()) {
                String job = rs.getString("JOB_NAME");
                jobsList.add(job);
                LOGGER.fine(String.format("Adding job '%s' from the list of registered projects cache", job));
            }
        } catch (SQLException sqlex) {
            LOGGER.fine("Failed to run distinct jobs query!");
            LOGGER.log(Level.SEVERE, "SQLException", sqlex);
        }

        return jobsList;
    }

    /**
     * Maintenance function to delete all inactive project cache tables
     *
     * @param dataSource
     * @param jobName
     * @throws SQLException
     */
    public static synchronized void deleteProjectCache(ConnectionPoolDataSource dataSource, String jobName) throws SQLException {
        ResultSet rs = null;

        try (
                // Get a connection from the pool
                Connection db = dataSource.getPooledConnection().getConnection();
                // First Check to see if the current project registry exists
                PreparedStatement select = db.prepareStatement(SELECT_REGISTRY_PROJECT, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                PreparedStatement delete = db.prepareStatement(DROP_REGISTRY_ENTRY);) {

            select.setString(1, jobName);
            rs = select.executeQuery();
            if (getRowCount(rs) > 0) {
                while (rs.next()) {
                    String cacheTableName = rs.getString("PROJECT_CACHE_TABLE");
                    executeStmt(dataSource, DROP_PROJECT_TABLE.replaceFirst("CM_PROJECT", cacheTableName));
                    delete.setString(1, cacheTableName);
                    delete.addBatch();
                }

                delete.executeBatch();
            }
        } catch (SQLException sqlex) {
            LOGGER.log(Level.FINE, "Failed to purge project ''{0}'' from Integrity SCM cache registry!", jobName);
            LOGGER.log(Level.SEVERE, "SQLException", sqlex);
        } finally {
            if (null != rs) {
                rs.close();
            }
        }
    }

    /**
     * Maintenance function to limit project cache to the most recent builds
     *
     * @param dataSource
     * @param jobName
     * @param buildNumber
     * @throws SQLException
     */
    public static synchronized void cleanupProjectCache(ConnectionPoolDataSource dataSource, String jobName, long buildNumber) throws SQLException {
        ResultSet rs = null;

        try (
                // Get a connection from the pool			
                Connection db = dataSource.getPooledConnection().getConnection();
                // First Check to see if the current project registry exists
                PreparedStatement select = db.prepareStatement(SELECT_REGISTRY_TABLE_DROP, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                PreparedStatement delete = db.prepareStatement(DROP_REGISTRY_ENTRY);) {

            select.setString(1, jobName);
            select.setLong(2, buildNumber);
            rs = select.executeQuery();

            int rowCount = getRowCount(rs);
            int deleteCount = 0;
            LOGGER.log(Level.FINE, "Cache entries for {0}/{1} = {2}", new Object[]{jobName, buildNumber, rowCount});
            while (rs.next()) {
                deleteCount = deleteOldCacheEntries(dataSource, jobName, delete, rs, deleteCount);
            }

            // Remove the cache entry regardless of the whether or not the cache table was purged
            if (deleteCount > 0) {
                delete.executeBatch();
            }
        } catch (SQLException sqlex) {
            LOGGER.fine(String.format("Failed to clear old cache for project '%s' from Integrity SCM cache registry!", jobName));
            LOGGER.log(Level.SEVERE, "SQLException", sqlex);
        } finally {
            if (null != rs) {
                rs.close();
            }
        }
    }

    private static int deleteOldCacheEntries(ConnectionPoolDataSource dataSource, String jobName,
            PreparedStatement delete, ResultSet rs, int deleteCount) throws SQLException {
        String cacheTableName = rs.getString("PROJECT_CACHE_TABLE");
        LOGGER.fine(String.format("Deleting old cache entry for %s/%s", jobName, cacheTableName));
        try {
            // Attempting to drop the old cache table						
            executeStmt(dataSource, DROP_PROJECT_TABLE.replaceFirst("CM_PROJECT", cacheTableName));
        } catch (SQLException sqlex) {
            // If this fails, then we'll have to investigate later...
            LOGGER.fine(String.format("Failed to drop table '%s' from Integrity SCM cache registry!", cacheTableName));
            LOGGER.log(Level.SEVERE, "SQLException", sqlex);
        }

        // Tag the cache entry for removal
        deleteCount++;
        delete.setString(1, cacheTableName);
        delete.addBatch();
        return deleteCount;
    }

    /**
     * Establishes a fresh set of Integrity SCM cache tables
     *
     * @param dataSource
     * @param tableName 
     * @return true/false depending on the success of the operation
     */
    public static synchronized boolean createCMProjectTables(ConnectionPoolDataSource dataSource, String tableName) {
        boolean tableCreated = false;
        try {
            if (executeStmt(dataSource, SELECT_MEMBER_1.replaceFirst("CM_PROJECT", tableName))) {
                tableCreated = dropAndCreateProjectCache(dataSource, tableName);
            }
        } catch (SQLException ex) {
            LOGGER.fine(ex.getMessage());
            try {
                LOGGER.fine(String.format("Integrity SCM cache table '%s' does not exist, creating...", tableName));
                tableCreated = executeStmt(dataSource, CREATE_PROJECT_TABLE.replaceFirst("CM_PROJECT", tableName));
            } catch (SQLException sqlex) {
                LOGGER.fine(String.format("Failed to create Integrity SCM project cache table '%s'", tableName));
                LOGGER.log(Level.SEVERE, "SQLException", sqlex);
                tableCreated = false;
            }
        }

        return tableCreated;
    }

    /**
     * Create or return existing CP cache table
     * 
     * @param i
     * @param configurationName
     * 
     * @param db Derby database connection
     * @return true/false depending on the success of the operation
     */
    public static synchronized boolean getCPCacheTable(ConnectionPoolDataSource dataSource,
        String cpCacheTableName)
    {
      boolean tableCreated = false;
      try
      {
        if (executeStmt(dataSource, SELECT_CP_1.replaceFirst("CM_PROJECT_CP", cpCacheTableName)))
        {
          LOGGER.fine("A prior set of PTC RV&S SCM CP cache table for this job detected.");
        }
      } catch (SQLException ex)
      {
        LOGGER.fine(ex.getMessage());
        try
        {
          LOGGER.fine(String.format("PTC RV&S SCM CP cache table '%s' does not exist, creating...",
              cpCacheTableName));
          tableCreated = executeStmt(dataSource,
              CREATE_PROJECT_CP_TABLE.replaceFirst("CM_PROJECT_CP", cpCacheTableName));
        } catch (SQLException sqlex)
        {
          LOGGER.fine(String.format("Failed to create PTC RV&S SCM project CP cache table '%s'",
              cpCacheTableName));
          LOGGER.log(Level.SEVERE, "SQLException", sqlex);
          tableCreated = false;
        }
      }

      return tableCreated;
    }


    private static boolean dropAndCreateProjectCache(ConnectionPoolDataSource dataSource, String tableName) {
        boolean tableCreated;
        try {
            LOGGER.fine("A prior set of Integrity SCM cache tables detected, dropping...");
            executeStmt(dataSource, DROP_PROJECT_TABLE.replaceFirst("CM_PROJECT", tableName));
            LOGGER.fine("Recreating a fresh set of Integrity SCM cache tables...");
            tableCreated = executeStmt(dataSource, CREATE_PROJECT_TABLE.replaceFirst("CM_PROJECT", tableName));
        } catch (SQLException ex) {
            LOGGER.fine(String.format("Failed to create Integrity SCM project cache table '%s'", tableName));
            LOGGER.log(Level.SEVERE, "SQLException", ex);
            tableCreated = false;
        }
        return tableCreated;
    }

    /**
     * Convenience function that converts a result set row into a Hashtable for
     * easy access
     *
     * @param rs ResultSet row object
     * @return a Map containing the non-null values for each column
     * @throws SQLException
     * @throws IOException
     */
    public static Map<CM_PROJECT, Object> getRowData(ResultSet rs) throws SQLException, IOException {
        HashMap<CM_PROJECT, Object> rowData = new HashMap<>();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        int columns = rsMetaData.getColumnCount();
        Object value;
        for (int i = 1; i <= columns; i++) {
            int columnType = rsMetaData.getColumnType(i);

            switch (columnType) {
                case java.sql.Types.ARRAY:
                    value = rs.getArray(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.BIGINT:
                case java.sql.Types.NUMERIC:
                case java.sql.Types.REAL:
                    value = rs.getLong(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.BLOB:
                    InputStream is = null;
                    try {
                        is = rs.getBlob(i).getBinaryStream();
                        byte[] bytes = IOUtils.toByteArray(is);
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), bytes);
                    } finally {
                        if (null != is) {
                            is.close();
                        }
                    }
                    break;

                case java.sql.Types.BOOLEAN:
                    value = rs.getBoolean(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.CLOB:
                    try (
                            BufferedReader reader = new java.io.BufferedReader(rs.getClob(i).getCharacterStream());) {
                        String line = null;
                        StringBuilder sb = new StringBuilder();
                        while (null != (line = reader.readLine())) {
                            sb.append(line).append(IntegritySCM.NL);
                        }
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), sb.toString());
                    }
                    break;

                case java.sql.Types.DATE:
                    value = rs.getDate(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.DECIMAL:
                    value = rs.getBigDecimal(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.DOUBLE:
                    value = rs.getDouble(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.FLOAT:
                    value = rs.getFloat(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.INTEGER:
                    value = rs.getInt(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.JAVA_OBJECT:
                    value = rs.getObject(i);
                    rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    break;

                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    value = rs.getShort(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.TIME:
                    value = rs.getTime(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                case java.sql.Types.TIMESTAMP:
                    value = rs.getTimestamp(i);
                    if (!rs.wasNull()) {
                        rowData.put(getEnum(rsMetaData.getColumnLabel(i)), value);
                    }
                    break;

                default:
                    value = rs.getString(i);
                    if (!rs.wasNull()) {
                        CM_PROJECT label = getEnum(rsMetaData.getColumnLabel(i));
                        rowData.put(label, value);
                    }
            }

        }

        return rowData;
    }

    /**
     * This function provides a count of the total number of rows in the
     * ResultSet
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    public static int getRowCount(ResultSet rs) throws SQLException {
        int rowCount = 0;
        int currentRow = rs.getRow();
        rowCount = rs.last() ? rs.getRow() : rowCount;
        if (currentRow == 0) {
            rs.beforeFirst();
        } else {
            rs.absolute(currentRow);
        }

        return rowCount;
    }

    /**
     * Attempts to fix known issues with characters that can potentially break
     * the change log xml
     *
     * @param desc Input comment string for the revision
     * @return Sanitized string that can be embedded within a CDATA tag
     */
    public static String fixDescription(String desc) {
        // Char 8211 which is a long dash causes problems for the change log XML, need to fix it!
        String description = desc.replace((char) 8211, '-');
        return description.replaceAll("<!\\[CDATA\\[", "< ! [ CDATA [").replaceAll("\\]\\]>", "] ] >");
    }

    /**
     * Compares this version of the project to a previous/new version to
     * determine what are the updates and what was deleted
     *
     * @param baselineProjectCache The previous baseline (build) for this
     * Integrity CM Project
     * @param projectCacheTable 
     * @param skipAuthorInfo 
     * @param api The current Integrity API Session to obtain the author
     * information
     * @return The total number of changes found in the comparison
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized int compareBaseline(String baselineProjectCache, String projectCacheTable, boolean skipAuthorInfo, APISession api) throws SQLException, IOException {
        // Re-initialize our return variable
        int changeCount = 0;

        Statement pjSelect = null;
        ResultSet rs = null;

        // Get a connection from our pool
        Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
        db.setAutoCommit(false);
        // Create the select statement for the previous baseline
        Statement baselineSelect = db.createStatement();
        ResultSet baselineRS = baselineSelect.executeQuery(DerbyUtils.BASELINE_SELECT.replaceFirst("CM_PROJECT", baselineProjectCache));

        try  
        {

            // Create a map to hold the old baseline for easy comparison
            HashMap<String, Map<CM_PROJECT, Object>> baselinePJ = new HashMap<>();
            while (baselineRS.next()) {
                Map<CM_PROJECT, Object> rowHash = DerbyUtils.getRowData(baselineRS);
                Map<CM_PROJECT, Object> memberInfo = new EnumMap<>(CM_PROJECT.class);
                memberInfo.put(CM_PROJECT.MEMBER_ID, (null == rowHash.get(CM_PROJECT.MEMBER_ID) ? "" : rowHash.get(CM_PROJECT.MEMBER_ID).toString()));
                memberInfo.put(CM_PROJECT.TIMESTAMP, (null == rowHash.get(CM_PROJECT.TIMESTAMP) ? "" : (Date) rowHash.get(CM_PROJECT.TIMESTAMP)));
                memberInfo.put(CM_PROJECT.DESCRIPTION, (null == rowHash.get(CM_PROJECT.DESCRIPTION) ? "" : rowHash.get(CM_PROJECT.DESCRIPTION).toString()));
                memberInfo.put(CM_PROJECT.AUTHOR, (null == rowHash.get(CM_PROJECT.AUTHOR) ? "" : rowHash.get(CM_PROJECT.AUTHOR).toString()));
                memberInfo.put(CM_PROJECT.CONFIG_PATH, (null == rowHash.get(CM_PROJECT.CONFIG_PATH) ? "" : rowHash.get(CM_PROJECT.CONFIG_PATH).toString()));
                memberInfo.put(CM_PROJECT.REVISION, (null == rowHash.get(CM_PROJECT.REVISION) ? "" : rowHash.get(CM_PROJECT.REVISION).toString()));
                memberInfo.put(CM_PROJECT.RELATIVE_FILE, (null == rowHash.get(CM_PROJECT.RELATIVE_FILE) ? "" : rowHash.get(CM_PROJECT.RELATIVE_FILE).toString()));
                memberInfo.put(CM_PROJECT.CHECKSUM, (null == rowHash.get(CM_PROJECT.CHECKSUM) ? "" : rowHash.get(CM_PROJECT.CHECKSUM).toString()));
                baselinePJ.put(rowHash.get(CM_PROJECT.NAME).toString(), memberInfo);
            }

            // Create the select statement for the current project
            pjSelect = db.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            String pjSelectSql = DerbyUtils.DELTA_SELECT.replaceFirst("CM_PROJECT", projectCacheTable);
            LOGGER.log(Level.FINE, "Attempting to execute query {0}", pjSelectSql);
            rs = pjSelect.executeQuery(pjSelectSql);

            // Now we will compare the adds and updates between the current project and the baseline
            for (int i = 1; i <= DerbyUtils.getRowCount(rs); i++) {
                // Move the cursor to the current record
                rs.absolute(i);
                Map<CM_PROJECT, Object> rowHash = DerbyUtils.getRowData(rs);
                // Obtain the member we're working with
                String memberName = rowHash.get(CM_PROJECT.NAME).toString();
                // Get the baseline project information for this member
                LOGGER.log(Level.FINE, "Comparing file against baseline {0}", memberName);
                Map<CM_PROJECT, Object> baselineMemberInfo = baselinePJ.get(memberName);
                // This file was in the previous baseline as well...
                if (null != baselineMemberInfo) {
                    // Did it change? Either by an update or roll back (update member revision)?
                    String oldRevision = baselineMemberInfo.get(CM_PROJECT.REVISION).toString();
                    if (!rowHash.get(CM_PROJECT.REVISION).toString().equals(oldRevision)) {
                        // Initialize the prior revision
                        rs.updateString(CM_PROJECT.OLD_REVISION.toString(), oldRevision);
                        // Initialize the author information as requested
                        if (!skipAuthorInfo) {
                            rs.updateString(CM_PROJECT.AUTHOR.toString(),
                                    IntegrityCMMember.getAuthor(api,
                                            rowHash.get(CM_PROJECT.CONFIG_PATH).toString(),
                                            rowHash.get(CM_PROJECT.MEMBER_ID).toString(),
                                            rowHash.get(CM_PROJECT.REVISION).toString()));
                        }
                        // Initialize the delta flag for this member
                        rs.updateShort(CM_PROJECT.DELTA.toString(), (short) 2);
                        LOGGER.log(Level.FINE, "... {0} revision changed - new revision is {1}", new Object[]{memberName, rowHash.get(CM_PROJECT.REVISION).toString()});
                        changeCount++;
                    } else {
                        // This member did not change, so lets copy its old author information
                        if (null != baselineMemberInfo.get(CM_PROJECT.AUTHOR)) {
                            rs.updateString(CM_PROJECT.AUTHOR.toString(), baselineMemberInfo.get(CM_PROJECT.AUTHOR).toString());
                        }
                        // Also, lets copy over the previous MD5 checksum
                        if (null != baselineMemberInfo.get(CM_PROJECT.CHECKSUM)) {
                            rs.updateString(CM_PROJECT.CHECKSUM.toString(), baselineMemberInfo.get(CM_PROJECT.CHECKSUM).toString());
                        }
                        // Initialize the delta flag
                        rs.updateShort(CM_PROJECT.DELTA.toString(), (short) 0);
                    }

                    // Remove this member from the baseline project hashtable, so we'll be left with items that are dropped
                    baselinePJ.remove(memberName);
                } else // We've found a new file
                {
                    // Initialize the author information as requested
                    if (!skipAuthorInfo) {
                        rs.updateString(CM_PROJECT.AUTHOR.toString(),
                                IntegrityCMMember.getAuthor(api,
                                        rowHash.get(CM_PROJECT.CONFIG_PATH).toString(),
                                        rowHash.get(CM_PROJECT.MEMBER_ID).toString(),
                                        rowHash.get(CM_PROJECT.REVISION).toString()));
                    }
                    // Initialize the delta flag for this member
                    rs.updateShort(CM_PROJECT.DELTA.toString(), (short) 1);
                    LOGGER.log(Level.FINE, "... {0} new file - revision is {1}", new Object[]{memberName, rowHash.get(CM_PROJECT.REVISION).toString()});
                    changeCount++;
                }

                // Update this row in the data source
                rs.updateRow();
            }

            // Now, we should be left with the drops.  Exist only in the old baseline and not the current one.
            Set<String> deletedMembers = baselinePJ.keySet();
            Iterator<String> dmIT = deletedMembers.iterator();
            while (dmIT.hasNext()) {
                changeCount++;
                String memberName = dmIT.next();
                Map<CM_PROJECT, Object> memberInfo = baselinePJ.get(memberName);

                // Add the deleted members to the database
                rs.moveToInsertRow();
                rs.updateShort(CM_PROJECT.TYPE.toString(), (short) 0);
                rs.updateString(CM_PROJECT.NAME.toString(), memberName);
                rs.updateString(CM_PROJECT.MEMBER_ID.toString(), memberInfo.get(CM_PROJECT.MEMBER_ID).toString());
                if (memberInfo.get(CM_PROJECT.TIMESTAMP) instanceof java.util.Date) {
                    Timestamp ts = new Timestamp(((Date) memberInfo.get(CM_PROJECT.TIMESTAMP)).getTime());
                    rs.updateTimestamp(CM_PROJECT.TIMESTAMP.toString(), ts);
                }
                rs.updateString(CM_PROJECT.DESCRIPTION.toString(), memberInfo.get(CM_PROJECT.DESCRIPTION).toString());
                rs.updateString(CM_PROJECT.AUTHOR.toString(), memberInfo.get(CM_PROJECT.AUTHOR).toString());
                rs.updateString(CM_PROJECT.CONFIG_PATH.toString(), memberInfo.get(CM_PROJECT.CONFIG_PATH).toString());
                rs.updateString(CM_PROJECT.REVISION.toString(), memberInfo.get(CM_PROJECT.REVISION).toString());
                rs.updateString(CM_PROJECT.RELATIVE_FILE.toString(), memberInfo.get(CM_PROJECT.RELATIVE_FILE).toString());
                rs.updateShort(CM_PROJECT.DELTA.toString(), (short) 3);
                rs.insertRow();
                rs.moveToCurrentRow();

                LOGGER.log(Level.FINE, "... {0} file dropped - revision was {1}", new Object[]{memberName, memberInfo.get(CM_PROJECT.REVISION).toString()});
            }
        } finally {
            if (null != rs) {
                rs.close();
            }
            if (null != pjSelect) {
                pjSelect.close();
            }

            
            baselineRS.close();
            baselineSelect.close();

            // Commit changes to the database...
            db.commit();

            db.setAutoCommit(true);
            db.close();
        }

        return changeCount;
    }

    /**
     * Parses the output from the si viewproject command to get a list of
     * members
     *
     * @param siProject
     * @param wit WorkItemIterator
     * @param listener
     * @throws APIException
     * @throws SQLException
     */
    public static synchronized void parseProject(IntegrityCMProject siProject, WorkItemIterator wit, TaskListener listener) throws APIException, SQLException {

        // Setup the Derby DB for this Project
        PreparedStatement insert = null;

                // Get a connection from our pool
        Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
        try {

            // WABco: disable autocommit to avoid thousands of them while inserting into db
            db.setAutoCommit(false);

            // Create a fresh set of tables for this project
            listener.getLogger().println("Derby.parse:  Create a fresh set of tables for this project");
            DerbyUtils.createCMProjectTables(DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource(), siProject.getProjectCacheTable());

            // Initialize the project config hash
            listener.getLogger().println("Derby.parse:  Initialize the project config hash");
            HashMap<String, String> pjConfigHash = new HashMap<>();
            // Add the mapping for the current project
            pjConfigHash.put(siProject.getProjectName(), siProject.getConfigurationPath());
            // Compute the project root directory
            String projectRoot = siProject.getProjectName().substring(0, siProject.getProjectName().lastIndexOf('/'));

            // Iterate through the list of members returned by the API
            String insertSQL = DerbyUtils.INSERT_MEMBER_RECORD.replaceFirst("CM_PROJECT", siProject.getProjectCacheTable());

            LOGGER.log(Level.FINE, "Attempting to execute query {0}", insertSQL);
            insert = db.prepareStatement(insertSQL);

            // Wabco:
            listener.getLogger().println("PTC viewproject - SCM Info:");

            while (wit.hasNext()) {
                WorkItem wi = wit.next();
                String entryType = (null != wi.getField("type") ? wi.getField("type").getValueAsString() : "");

                switch (wi.getModelType()) {
                    case SIModelTypeName.SI_SUBPROJECT:
                        // Ignore p e n d i n g subprojects in the tree...
                        if (entryType.equalsIgnoreCase("pending-sharesubproject")) {
                            LOGGER.log(Level.WARNING, "Skipping {0} {1}", new Object[]{entryType, wi.getId()});
                            listener.getLogger().println("Derby.parse:  Ignoring " + entryType + " " + wi.getId());
                        } else {
                            // Save the configuration path for the current subproject, using the canonical path name
                            pjConfigHash.put(wi.getField("name").getValueAsString(), wi.getId());
                            // Save the relative directory path for this subproject
                            String pjDir = wi.getField("name").getValueAsString().substring(projectRoot.length());
                            pjDir = pjDir.substring(0, pjDir.lastIndexOf('/'));
                            // Save this directory entry
                            insert.clearParameters();
                            insert.setShort(1, (short) 1);							// Type
                            insert.setString(2, wi.getField("name").getValueAsString());			// Name
                            insert.setString(3, wi.getId());							// MemberID
                            insert.setTimestamp(4, new Timestamp(Calendar.getInstance().getTimeInMillis()));	// Timestamp
                            insert.setClob(5, new StringReader(""));						// Description
                            insert.setString(6, wi.getId());							// ConfigPath
                            
                            String subProjectRev = "";
                            if (wi.contains("memberrev")) {
                                subProjectRev = wi.getField("memberrev").getItem().getId();
                            }
                            insert.setString(7, subProjectRev);							// Revision
                            insert.setString(8, pjDir);								// RelativeFile
                            
                            insert.executeUpdate();
                        }   break;
                    case SIModelTypeName.MEMBER:
                        // Ignore certain p e n d i n g operations
                        if (entryType.endsWith("in-pending-sub")
                                || entryType.equalsIgnoreCase("pending-add")
                                || entryType.equalsIgnoreCase("pending-move-to-update")
                                || entryType.equalsIgnoreCase("pending-rename-update")) {
                            LOGGER.log(Level.WARNING, "Skipping {0} {1}", new Object[]{entryType, wi.getId()});
                            listener.getLogger().println("Derby.parse:  Ignoring " + entryType + " " + wi.getId());
                        } else {
                            // Figure out this member's parent project's canonical path name
                            String parentProject = wi.getField("parent").getValueAsString();
                            // Save this member entry
                            String memberName = wi.getField("name").getValueAsString();
                            // Figure out the full member path
                            LOGGER.log(Level.FINE, "Member context: {0}", wi.getContext());
                            LOGGER.log(Level.FINE, "Member parent: {0}", parentProject);
                            LOGGER.log(Level.FINE, "Member name: {0}", memberName);
                            
                            // Process this member only if we can figure out where to put it in the workspace
                            if (memberName.startsWith(projectRoot)) {
                                String description = "";
                                // Per JENKINS-19791 some users are getting an exception when attempting
                                // to read the 'memberdescription' field in the API response. This is an
                                // attempt to catch the exception and ignore it...!
                                try {
                                    if (null != wi.getField("memberdescription") && null != wi.getField("memberdescription").getValueAsString()) {
                                        description = fixDescription(wi.getField("memberdescription").getValueAsString());
                                    }
                                } catch (NoSuchElementException e) {
                                    // Ignore exception
                                    LOGGER.log(Level.WARNING, "Cannot obtain the value for ''memberdescription'' in API response for member: {0}", memberName);
                                    LOGGER.info("API Response has the following fields available: ");
                                    for (@SuppressWarnings("unchecked")
                                    final Iterator<Field> fieldsIterator = wi.getFields(); fieldsIterator.hasNext();) {
                                        Field apiField = fieldsIterator.next();
                                        LOGGER.log(Level.INFO, "Name: {0}, Value: {1}", new Object[]{apiField.getName(), apiField.getValueAsString()});
                                    }
                                }
                                
                                Date timestamp = new Date();
                                // Per JENKINS-25068 some users are getting a null pointer exception when attempting
                                // to read the 'membertimestamp' field in the API response. This is an attempt to work around it!
                                try {
                                    Field timeFld = wi.getField("membertimestamp");
                                    if (null != timeFld && null != timeFld.getDateTime()) {
                                        timestamp = timeFld.getDateTime();
                                    }
                                } catch (Exception e) {
                                    // Ignore exception
                                    LOGGER.log(Level.WARNING, "Cannot obtain the value for ''membertimestamp'' in API response for member: {0}", memberName);
                                    LOGGER.log(Level.WARNING, "Defaulting ''membertimestamp'' to now - {0}", timestamp);
                                }
                                
                                insert.clearParameters();
                                insert.setShort(1, (short) 0);                                      // Type
                                insert.setString(2, memberName);                                    // Name
                                insert.setString(3, wi.getId());                                    // MemberID
                                insert.setTimestamp(4, new Timestamp(timestamp.getTime()));         // Timestamp
                                insert.setClob(5, new StringReader(description));                   // Description
                                insert.setString(6, pjConfigHash.get(parentProject));               // ConfigPath
                                insert.setString(7, wi.getField("memberrev").getItem().getId());    // Revision
                                insert.setString(8, memberName.substring(projectRoot.length()));    // RelativeFile (for workspace)
                                
                                // Wabco-Feature: always print all items and main configuration information to the log
                                listener.getLogger().println("\t"
                                        //									+ "Type:" +  (short)0                               // Type
                                        + memberName.substring(projectRoot.length()) + '\t' // RelativeFile (for workspace)
                                        //									+ memberName + "\t"                                 // Name
                                        + wi.getField("memberrev").getItem().getId() + '\t' // Revision
                                        //									+ wi.getId() + '\t'                                 // MemberID
                                        + new Timestamp(timestamp.getTime()) // Timestamp
                                        //									+ new StringReader(description)                     // Description
                                        //									+ pjConfigHash.get(parentProject)                   // ConfigPath
                                );
                                /// Wabco:
                                insert.executeUpdate();
                            } else {
                                // Issue warning...
                                LOGGER.log(Level.WARNING, "Skipping {0} it doesn''t appear to exist within this project {1}!", new Object[]{memberName, projectRoot});
                            }
                        }   break;
                    default:
                        LOGGER.log(Level.WARNING, "View project output contains an invalid model type: {0}", wi.getModelType());
                        break;
                }
            }

        } finally {
            if (null != insert) { insert.close(); }

            // Commit to the database
            db.commit();
            db.setAutoCommit(true);
            db.close();
        }

        // Log the completion of this operation
        LOGGER.log(Level.FINE, "Parsing project {0} complete!", siProject.getConfigurationPath());
    }

    /**
     * Updates the author information for all the members in the project
     *
     * @param projectCacheTable
     * @param api
     * @param listener
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized void primeAuthorInformation(String projectCacheTable, APISession api, TaskListener listener) throws SQLException, IOException {
        ResultSet rs = null;
        // Get a connection from our pool
        Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
        // Create the select statement for the current project
        Statement authSelect = db.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        try 
        {

            // Wabco: #Issue1 disable autocommit for faster db interactions, see updateRow
            db.setAutoCommit(false);

            listener.getLogger().println("PTC Plugin: Begin Prime Author Infomation (init or refresh complete history cache)...");
            listener.getLogger().print("PTC Plugin: Number of elements to process: ");

            rs = authSelect.executeQuery(DerbyUtils.AUTHOR_SELECT_COUNT.replaceFirst("CM_PROJECT", projectCacheTable));

            int totalMembersInProjectTodo = 0;
            int filesDone = -1;
            if (!rs.wasNull()) {
                rs.next();
                totalMembersInProjectTodo = rs.getInt(1);
                listener.getLogger().println(totalMembersInProjectTodo);
            } else {
                listener.getLogger().println("error no sql count possible.");
            }

            rs = authSelect.executeQuery(DerbyUtils.AUTHOR_SELECT.replaceFirst("CM_PROJECT", projectCacheTable));

            int perc = -1;
            while (rs.next()) {
                filesDone += 10;      // report each 10 % percent only
                int pnew = (filesDone / totalMembersInProjectTodo);
                if (pnew > perc) {
                    perc = pnew;
                    listener.getLogger().println(perc*10 + "% done ...");
                }

                Map<CM_PROJECT, Object> rowHash = DerbyUtils.getRowData(rs);
                rs.updateString(CM_PROJECT.AUTHOR.toString(),
                        IntegrityCMMember.getAuthor(api,
                                rowHash.get(CM_PROJECT.CONFIG_PATH).toString(),
                                rowHash.get(CM_PROJECT.MEMBER_ID).toString(),
                                rowHash.get(CM_PROJECT.REVISION).toString()));
                rs.updateRow();
            }

        }
        finally {
            // Release the result set
            if (null != rs) {
                rs.close();
            }
            authSelect.close();
            // Commit the updates
            db.commit();
            db.setAutoCommit(true);
            db.close();
        }
    }

    /**
     * Updates the underlying Integrity SCM Project table cache with the new
     * checksum information
     *
     * @param projectCacheTable
     * @param checksumHash Checksum HashMap generated from a checkout operation
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized void updateChecksum(String projectCacheTable, ConcurrentMap<String, String> checksumHash) throws SQLException, IOException {
        // Get a connection from our pool
        Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
        try (
                // Create the select statement for the current project
                Statement checksumSelect = db.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = checksumSelect.executeQuery(DerbyUtils.CHECKSUM_UPDATE.replaceFirst("CM_PROJECT", projectCacheTable));) {
            db.setAutoCommit(false);
            while (rs.next()) {
                Map<CM_PROJECT, Object> rowHash = DerbyUtils.getRowData(rs);
                String newChecksum = checksumHash.get(rowHash.get(CM_PROJECT.NAME).toString());
                if (null != newChecksum && newChecksum.length() > 0) {
                    rs.updateString(CM_PROJECT.CHECKSUM.toString(), newChecksum);
                    rs.updateRow();
                }
            }

            // Commit the updates
            db.commit();
            db.setAutoCommit(true);
            db.close();
        }
    }

    /**
     * Project access function that returns the state of the current project
     * NOTE: For maximum efficiency, this should be called only once and after
     * the compareBasline() has been invoked!
     *
     * @param projectCacheTable
     * @return A List containing every member in this project, including any
     * dropped artifacts
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized List<Map<CM_PROJECT, Object>> viewProject(String projectCacheTable) throws SQLException, IOException {
        // Initialize our return variable
        List<Map<CM_PROJECT, Object>> projectMembersList = new ArrayList<>();

        try (
                // Get a connection from our pool
                Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
                Statement stmt = db.createStatement();
                ResultSet rs = stmt.executeQuery(DerbyUtils.PROJECT_SELECT.replaceFirst("CM_PROJECT", projectCacheTable));) {
            while (rs.next()) {
                projectMembersList.add(DerbyUtils.getRowData(rs));
            }
        }

        return projectMembersList;
    }

    /**
     * Project access function that returns the state of the current project
     * NOTE: For maximum efficiency, this should be called only once and after
     * the compareBasline() has been invoked!
     *
     * @param projectCacheTable
     * @return A List containing every subproject in this project
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized List<Map<CM_PROJECT, Object>> viewSubProjects(String projectCacheTable) throws SQLException, IOException {
        // Initialize our return variable
        List<Map<CM_PROJECT, Object>> subprojectsList = new ArrayList<>();

        try (
                // Get a connection from our pool
                Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
                Statement stmt = db.createStatement();
                ResultSet rs = stmt.executeQuery(DerbyUtils.SUB_PROJECT_SELECT.replaceFirst("CM_PROJECT", projectCacheTable));) {
            while (rs.next()) {
                subprojectsList.add(DerbyUtils.getRowData(rs));
            }
        }

        return subprojectsList;
    }

    /**
     * Returns a string list of relative paths to all directories in this
     * project
     *
     * @param projectCacheTable
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized List<String> getDirList(String projectCacheTable) throws SQLException, IOException {
        // Initialize our return variable
        List<String> dirList = new ArrayList<>();

        try (
                // Get a connection from our pool
                Connection db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection().getConnection();
                Statement stmt = db.createStatement();
                ResultSet rs = stmt.executeQuery(DerbyUtils.DIR_SELECT.replaceFirst("CM_PROJECT", projectCacheTable));) {
            while (rs.next()) {
                Map<CM_PROJECT, Object> rowData = DerbyUtils.getRowData(rs);
                dirList.add(rowData.get(CM_PROJECT.RELATIVE_FILE).toString());
            }
        }

        return dirList;
    }

    /**
     * Cache the list of CPs (all states except "closed"). THis is to ensure that all CPs for a
     * project are tracked by Jenkins
     * 
     * @param cpCacheTable
     * @param cp
     * @param cpState
     * @param operation
     * @throws SQLException
     */
    public static Set<String> doCPCacheOperations(String cpCacheTable, String cp, String cpState,
        String operation) throws SQLException
    {
      Set<String> cachedCPIds = null;

      // Initialize our db connection
      Connection db = null;
      PreparedStatement stmt = null;
      PreparedStatement cachedCPSelect = null;
      ResultSet cachedCPRS = null;

      try
      {
        // Get a connection from our pool
        db = DescriptorImpl.INTEGRITY_DESCRIPTOR.getDataSource().getPooledConnection()
            .getConnection();
        if (operation.equalsIgnoreCase(IAPIFields.ADD_OPERATION))
        {
          // Add CP entry to cache
          String insertSQL = DerbyUtils.INSERT_CP_RECORD.replaceFirst("CM_PROJECT_CP", cpCacheTable);
          stmt = db.prepareStatement(insertSQL);
          stmt.clearParameters();
          stmt.setString(1, cp);
          stmt.setString(2, cpState);
          LOGGER.log(Level.FINE, "Updating CP Cache with CP : " + cp + ", State : " + cpState);
          stmt.executeUpdate();
        } else if (operation.equalsIgnoreCase(IAPIFields.DELETE_OPERATION))
        {
          // delete the CP entry from cache
          String deleteSQL = DerbyUtils.DELETE_CP_RECORD.replaceFirst("CM_PROJECT_CP", cpCacheTable);
          stmt = db.prepareStatement(deleteSQL);
          stmt.setString(1, cp);
          stmt.executeUpdate();

        } else if (operation.equalsIgnoreCase(IAPIFields.GET_OPERATION))
        {
          // Retrieve the list of CPs from the Derby DB
          cachedCPIds = new HashSet<String>();
          cachedCPSelect = db.prepareStatement(DerbyUtils.CP_SELECT.replaceFirst("CM_PROJECT_CP", cpCacheTable));
          cachedCPRS = cachedCPSelect.executeQuery();
          
          while (cachedCPRS.next())
          {
            cachedCPIds.add(cachedCPRS.getString(1));
          }
        } else
        {
          // Do nothing
          LOGGER.log(Level.SEVERE, "Operation :" + operation
              + " unsupported for updating CP Cache with CP : " + cp + ", State : " + cpState);
        }

        db.commit();
      } finally
      {
        // Close the insert statement
        if (null != stmt)
          stmt.close();

        // Close the database connection
        if (null != db)
          db.close();
      }
      return cachedCPIds;
    }
}
