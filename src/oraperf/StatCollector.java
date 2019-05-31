package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import oracle.jdbc.OracleConnection;

public class StatCollector
        extends Thread {

    SL4JLogger lg;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final int threadType;
    private final String dbUserName;
    private final String dbPassword;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private Connection con;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private boolean shutdown = false;

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, int runTType, BlockingQueue<OraCkhMsg> queue) {
        dbConnectionString      = inputString;
        dbUniqueName            = inputString.split("/")[1];
        dbHostName              = inputString.split(":")[0];
        dbUserName              = dbUSN;
        dbPassword              = dbPWD;
        threadType              = runTType;
        ckhQueue                = queue;
    }

    private void cleanup() {
        shutdown = true;
        try {
            if ((con != null) && (!con.isClosed())) {
                con.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString 
                    + "\t" + "error during ORADB resource cleanups"
                    + "\t" + e.getMessage()
            );

            //e.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Properties props = new Properties();
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_USER_NAME, dbUserName);
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_PASSWORD, dbPassword);
            //props.setProperty(OracleConnection.CONNECTION_PROPERTY_DEFAULT_USE_NIO, "true");
            //props.setProperty(OracleConnection.CONNECTION_PROPERTY_NET_KEEPALIVE, "true");
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT, "60000");
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_READ_TIMEOUT, "180000");
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_AUTOCOMMIT, "false");
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + dbConnectionString, props);
            //con.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "cannot load Oracle driver!"
            );
            shutdown = true;
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot initiate connection to target Oracle database"
                    + "\t" + e.getMessage()
            );
            //e.printStackTrace();

            shutdown = true;
        }
    }
    
    private int getVersion () {
            int version = 0;
            ResultSet rs = null;
            Statement stmt = null;
            try{
                stmt = con.createStatement();
                rs = stmt.executeQuery("select to_number(substr(version,1,instr(version,'.',1,1)-1)) from v$instance");
                if(rs.next()){
                    version = rs.getInt(1);
                }
                rs.close();
                stmt.close();
                return version;
            }catch(Exception e){
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot get version from database" 
                    + "\t" + e.getMessage()
            );                
                //e.printStackTrace();
                if( rs != null || !rs.isClosed()){
                    rs.close();
                }
                if ( stmt != null || ! stmt.isClosed()){
                    stmt.close();
                }
                version = 0;
            }finally{
                return version;
            }
    }
    
    @Override
    public void run() {
        int dbVersion = 0;
        
        Thread.currentThread().setName(dbHostName+"@"+dbUniqueName+"@"+String.valueOf(threadType));
        
        lg = new SL4JLogger();

        openConnection();
        
        if(! shutdown){
            dbVersion = getVersion();
        }
        if (!shutdown && dbVersion>0) {
            try {
                switch (threadType) {
                    case 0:
                        WaitsCollector waits = new WaitsCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        waits.RunCollection();
                        break;
                    case 1:
                        SesCollector ses = new SesCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        ses.RunCollection();
                        break;
                    case 2:
                        SysCollector sys = new SysCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        sys.RunCollection();
                        break;
                    case 3:
                        SQLCollector sql = new SQLCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        sql.RunCollection();
                        break;
                    default:
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString 
                                + "\t" + "unknown thread type provided!"
                        );
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cleanup();
        }
    }
}
