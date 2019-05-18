package oraperf;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SysCollector {
    SL4JLogger lg;
    private final int SECONDSBETWEENSYSSTATSSNAPS = 120;
    private final int RSSYSTEMSTAT = 2;
    private final int RSSQLPHV = 5;
    private final int RSSQLTEXT = 6;
    private final int RSSTATNAME = 7;
    private final int RSFILESSIZE = 10;
    private final int RSSEGMENTSSIZE = 11;
    private final int dbVersion;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final Connection con;
    private PreparedStatement oraSysStatsPreparedStatement;
    private PreparedStatement oraSQLTextsPreparedStatement;
    private PreparedStatement oraSQLPlansPreparedStatement;
    private PreparedStatement oraStatNamesPreparedStatement;
    private PreparedStatement oraSegmentsSizePreparedStatement;
    private PreparedStatement oraFilesSizePreparedStatement;
    private long currentDateTime;
    private boolean shutdown = false;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private static final String ORASYSSTATSQUERY = "select statistic#,value from v$sysstat where value<>0";
    private static final String ORASQLTEXTSQUERY = "select sql_id,sql_text from v$sqlarea";
    private static final String ORASQLTEXTSQUERYCDB = "select sql_id,sql_text from v$sqlarea where con_id=sys_context('USERENV','CON_ID')";
    private static final String ORASQLPLANSQUERY = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0";
    private static final String ORASQLPLANSQUERYCDB = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0 and con_id=sys_context('USERENV','CON_ID')";    
    private static final String ORASTATNAMESQUERY = "select statistic#,name from v$statname";
    private static final String ORAFILESSIZEQUERY = 
            "select 0 file_type,file_id,file_name,round(bytes/1024/1024) sizemb,substr(autoextensible,1,1),round(maxbytes/1024/1024) maxmb,tablespace_name from dba_data_files " +
            "union all " +
            "select 1 file_type,file_id,file_name,round(bytes/1024/1024) sizemb,substr(autoextensible,1,1),round(maxbytes/1024/1024) maxmb,tablespace_name from dba_temp_files";    
    private static final String ORASEGMENTSSIZEQUERY = 
            "select owner,segment_name,nvl(partition_name,'-'),segment_type,tablespace_name,round(bytes/1024/1024) sizemb " +
            "from dba_segments " +
            "where bytes>1024*1024*64 and segment_type not in ('TYPE2 UNDO','TEMPORARY')";
    
    public SysCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version){
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;  
        dbVersion               = version;
    }

    private void cleanup() {
        try {
            if ((oraSegmentsSizePreparedStatement != null) && (!oraSegmentsSizePreparedStatement.isClosed())) {
                oraSegmentsSizePreparedStatement.close();
            }                        
            if ((oraFilesSizePreparedStatement != null) && (!oraFilesSizePreparedStatement.isClosed())) {
                oraFilesSizePreparedStatement.close();
            }            
            if ((oraSysStatsPreparedStatement != null) && (!oraSysStatsPreparedStatement.isClosed())) {
                oraSysStatsPreparedStatement.close();
            }
            if ((oraSQLTextsPreparedStatement != null) && (!oraSQLTextsPreparedStatement.isClosed())) {
                oraSQLTextsPreparedStatement.close();
            }
            if ((oraSQLPlansPreparedStatement != null) && (!oraSQLPlansPreparedStatement.isClosed())) {
                oraSQLPlansPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    dbConnectionString + "\t"+"Error durring ORADB resource cleanups!"
                    + "\t" + e.getMessage()
            );

            //e.printStackTrace();
        }
    }    
    
    private List getSysStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(Integer.valueOf(rs.getInt(1)));
                rowList.add((long) new BigDecimal(rs.getDouble(2)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }
    
    private List getSQlTextsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }

    private List getFilesSizeListFromRS(ResultSet rs){
        List<List> outList = new ArrayList();        
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getInt(1));
                rowList.add(rs.getInt(2));
                rowList.add(rs.getString(3));
                rowList.add(rs.getLong(4));
                rowList.add(rs.getString(5));
                rowList.add(rs.getLong(6));
                rowList.add(rs.getString(7));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }        
        return outList;
    }
    
    private List getSegmentsSizeListFromRS(ResultSet rs){
        List<List> outList = new ArrayList();  
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                rowList.add(rs.getString(3));
                rowList.add(rs.getString(4));
                rowList.add(rs.getString(5));
                rowList.add(rs.getLong(6));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }               
        return outList;
    }
    
    private List getSQlPlansListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getLong(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }    

    private List getStatNamesListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getInt(1));
                rowList.add(rs.getString(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }

    
    public void RunCollection() throws InterruptedException{
        lg = new SL4JLogger();
        long begints,endts;
        try{
            oraSysStatsPreparedStatement = con.prepareStatement(ORASYSSTATSQUERY);
            oraSysStatsPreparedStatement.setFetchSize(500);
            oraSQLTextsPreparedStatement = con.prepareStatement((dbVersion>=12)? ORASQLTEXTSQUERYCDB : ORASQLTEXTSQUERY);
            oraSQLTextsPreparedStatement.setFetchSize(1000);
            oraStatNamesPreparedStatement = con.prepareStatement(ORASTATNAMESQUERY);
            oraStatNamesPreparedStatement.setFetchSize(1000);
            oraSQLPlansPreparedStatement = con.prepareStatement((dbVersion>=12)? ORASQLPLANSQUERYCDB : ORASQLPLANSQUERY);
            oraSQLPlansPreparedStatement.setFetchSize(1000);            
            oraFilesSizePreparedStatement = con.prepareStatement(ORAFILESSIZEQUERY);
            oraFilesSizePreparedStatement.setFetchSize(100);
            oraSegmentsSizePreparedStatement = con.prepareStatement(ORASEGMENTSSIZEQUERY);
            oraSegmentsSizePreparedStatement.setFetchSize(100);            
        }catch(SQLException e){
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Cannot prepare statements for  Oracle database: " + dbConnectionString
            ); 
            shutdown = true;
        }
        int snapcounter = 0;
        try {
            oraStatNamesPreparedStatement.execute();
            ckhQueue.put(new OraCkhMsg(RSSTATNAME, 0, dbUniqueName, null,
                    getStatNamesListFromRS(oraStatNamesPreparedStatement.getResultSet())));

            oraStatNamesPreparedStatement.close();
        } catch (Exception e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    dbConnectionString + "\t"+"Error processing statistics names!"
            );

            shutdown = true;
        }
        while (!shutdown) {
            begints = System.currentTimeMillis();
            try {
                currentDateTime = Instant.now().getEpochSecond();
                oraSysStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSYSTEMSTAT, currentDateTime, dbUniqueName, dbHostName,
                        getSysStatsListFromRS(oraSysStatsPreparedStatement.getResultSet())));

                oraSysStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        dbConnectionString + "\t"+"Error processing system statistics!"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
            if( ( (snapcounter>=5) && (snapcounter!=30) && (snapcounter % 5 == 0) || (snapcounter==0) ) ){
                if(!shutdown){
                    try{
                        oraFilesSizePreparedStatement.execute();
                        ckhQueue.put(new OraCkhMsg(RSFILESSIZE, currentDateTime, dbUniqueName, dbHostName,
                            getFilesSizeListFromRS(oraFilesSizePreparedStatement.getResultSet())));

                        oraFilesSizePreparedStatement.clearWarnings();                        
                    } catch (SQLException e) {
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Error processing files size!"
                                + "\t" + e.getMessage()
                        );
                        shutdown = true;
                        //e.printStackTrace();
                    }                    
                }
                if(!shutdown){
                    try{
                        oraSegmentsSizePreparedStatement.execute();
                        ckhQueue.put(new OraCkhMsg(RSSEGMENTSSIZE, currentDateTime, dbUniqueName, dbHostName,
                            getSegmentsSizeListFromRS(oraSegmentsSizePreparedStatement.getResultSet())));

                        oraSegmentsSizePreparedStatement.clearWarnings();                         
                    } catch (SQLException e) {
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Error processing segments size!"
                                + "\t" + e.getMessage()
                        );
                        shutdown = true;
                        //e.printStackTrace();
                    }                                        
                }          
            }
            else if (snapcounter == 30) {
                snapcounter = 0;
                if (!shutdown) {
                    try {
                        oraSQLPlansPreparedStatement.execute();
                        ckhQueue.put(new OraCkhMsg(RSSQLPHV, 0, null, null,
                                getSQlPlansListFromRS(oraSQLPlansPreparedStatement.getResultSet())));

                        oraSQLPlansPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Error processing sql plan hash values!"
                                + "\t" + e.getMessage()
                        );

                        shutdown = true;
                        //e.printStackTrace();
                    }
                }
                if (!shutdown) {
                    try {
                        oraSQLTextsPreparedStatement.execute();
                        ckhQueue.put(new OraCkhMsg(RSSQLTEXT, 0, null, null,
                                getSQlTextsListFromRS(oraSQLTextsPreparedStatement.getResultSet())));

                        oraSQLTextsPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Error processing sql texts!"
                                + "\t" + e.getMessage()
                        );

                        shutdown = true;
                        //e.printStackTrace();
                    }
                }
            } 
            endts = System.currentTimeMillis();
            if (endts - begints < SECONDSBETWEENSYSSTATSSNAPS*1000L) {
                TimeUnit.SECONDS.sleep(SECONDSBETWEENSYSSTATSSNAPS - (int) ((endts - begints) / 1000L));
            }
            snapcounter++;
        }   
        cleanup();
    }
}
