package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import oracle.jdbc.OracleConnection;

public class StatCollector
        extends Thread implements Configurable {

    private final SL4JLogger lg;
    private final int threadType;
    private final String dbUserName;
    private final String dbPassword;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final BlockingQueue<OraCkhMsg> ckhQueue;

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, int runTType, BlockingQueue<OraCkhMsg> queue) {
        dbConnectionString      = inputString;
        dbUniqueName            = inputString.split("/")[1];
        dbHostName              = inputString.split(":")[0];
        dbUserName              = dbUSN;
        dbPassword              = dbPWD;
        threadType              = runTType;
        ckhQueue                = queue;
        lg                      = new SL4JLogger();
    }

    private void cleanup(Connection con) {
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

    private Connection openConnection() {
        Connection con=null;
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
            //shutdown = true;
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot initiate connection to target Oracle database"
                    + "\t" + e.getMessage()
            );
            //e.printStackTrace();

            //shutdown = true;
        }finally{
            return con;
        }
        
    }
    
    private int getVersion (Connection con) {
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
        Connection con=null;
        boolean shutdown = false;
        int dbVersion = 0;
        
        Thread.currentThread().setName(dbHostName+"@"+dbUniqueName+"@"+String.valueOf(threadType));

        con = openConnection();
        
        if(!(con==null)){
            dbVersion = getVersion(con);
        }
        if (!shutdown && dbVersion>0) {
            try {
                switch (threadType) {
                    case THREADWAITS:
                        WaitsCollector waits = new WaitsCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        waits.RunCollection();
                        break;
                    case THREADSESSION:
                        SesCollector ses = new SesCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        ses.RunCollection();
                        break;
                    case THREADSYSTEM:
                        SysCollector sys = new SysCollector(con, ckhQueue, dbUniqueName, dbHostName, dbConnectionString, dbVersion);
                        sys.RunCollection();
                        break;
                    case THREADSQL:
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
            cleanup(con);
        }
    }
}
