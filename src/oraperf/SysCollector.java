package oraperf;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SysCollector implements Configurable {

    private final SL4JLogger lg;
    private final int dbVersion;
    private final int dbRole;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final Connection con;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private static final String ORASYSSTATSQUERY = "select statistic#,value from v$sysstat where value<>0";
    private static final String ORASTATNAMESQUERY = "select statistic#,name from v$statname";
    private static final String ORAFILESSIZEQUERY
            = "select 0 file_type,file_id,file_name,round(bytes/1024/1024) sizemb,substr(autoextensible,1,1),round(maxbytes/1024/1024) maxmb,tablespace_name,upper(substr(contents,1,1)) "
            + "from dba_data_files join dba_tablespaces using(tablespace_name) "
            + "union all "
            + "select 1 file_type,file_id,file_name,round(bytes/1024/1024) sizemb,substr(autoextensible,1,1),round(maxbytes/1024/1024) maxmb,tablespace_name,'T' from dba_temp_files";
    private static final String ORASEGMENTSSIZEQUERY
            = "select owner,segment_name,nvl(partition_name,'-'),segment_type,tablespace_name,round(bytes/1024/1024) sizemb "
            + "from dba_segments "
            + "where segment_type not in ('TYPE2 UNDO','TEMPORARY')";
    private static final String ORAIOFILESTATSQUERY = "select /*+ rule */filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from v$iostat_file a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6)";
    
    //private static final String ORAIOFILESTATSQUERY = "with t as ( " +
    //    "    select /*+ materialize */ name,file_no,filetype_id " +
    //    "    from ( " +
    //    "        select name,file# file_no, 2 filetype_id from v$datafile " +
    //    "        union all " +
    //    "        select name,file# file_no, 6 filetype_id from v$tempfile " +
    //    "    )" +
    //    ") " +
    //    "select /*+ rule */ " +
    //    "filetype_name,coalesce(b.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime " +
    //    "from v$iostat_file a " +
    //    "left join t b using(file_no,filetype_id)";
        
    private static final String ORAIOFILESTATSQUERYCDB = "select /*+ rule */filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from v$iostat_file a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6) where a.con_id=sys_context('USERENV','CON_ID')";
    private static final String ORAIOFUNCTIONSTATSQUERY = "select function_name,filetype_name,small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,number_of_waits,wait_time from v$iostat_function_detail";

    public SysCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version, int role) {
        ckhQueue            = queue;
        con                 = conn;
        dbConnectionString  = connstr;
        dbUniqueName        = dbname;
        dbHostName          = dbhost;
        dbVersion           = version;
        dbRole              = role;
        lg                  = new SL4JLogger();
    }

    private void cleanup(
        PreparedStatement oraSegmentsSizePreparedStatement,
        PreparedStatement oraFilesSizePreparedStatement,
        PreparedStatement oraSysStatsPreparedStatement,
        PreparedStatement oraIOFunctionStatsPreparedStatement,
        PreparedStatement oraIOFileStatsPreparedStatement
    ) {
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

            if ((oraIOFileStatsPreparedStatement != null) && (!oraIOFileStatsPreparedStatement.isClosed())) {
                oraIOFileStatsPreparedStatement.close();
            }
            if ((oraIOFunctionStatsPreparedStatement != null) && (!oraIOFunctionStatsPreparedStatement.isClosed())) {
                oraIOFunctionStatsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error during ORADB resource cleanups"
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
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from system stats resultset"
                    + "\t" + e.getMessage()
            );
            outList.clear();
        }
        return outList;
    }

    private List getIOFileStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                rowList.add(new BigDecimal(rs.getDouble(3)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(4)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(5)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(6)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(7)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(8)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(9)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(10)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(11)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(12)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(13)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(14)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(15)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(16)).longValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from io file stats resultset"
                    + "\t" + e.getMessage()
            );
            outList.clear();
        }
        return outList;
    }

    private List getIOFunctionStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                rowList.add(new BigDecimal(rs.getDouble(3)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(4)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(5)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(6)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(7)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(8)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(9)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(10)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(11)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(12)).longValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from io function stats resultset"
                    + "\t" + e.getMessage()
            );
            outList.clear();
        }
        return outList;
    }



    private List getFilesSizeListFromRS(ResultSet rs) {
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
                rowList.add(rs.getString(8));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from files resultset"
                    + "\t" + e.getMessage()
            );
            outList.clear();
        }
        return outList;
    }

    private List getSegmentsSizeListFromRS(ResultSet rs) {
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
                if (rs.getLong(6) > 0){
                    outList.add(rowList);    
                }
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from segments resultset"
                    + "\t" + e.getMessage()
            );
            outList.clear();
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
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from stat names resultset"
                    + "\t" + e.getMessage()
            );
            outList.clear();
        }
        return outList;
    }

    private boolean collectIOFileStats(boolean shutdown,long currentDateTime,PreparedStatement oraIOFileStatsPreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraIOFileStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSIOFILESTAT, currentDateTime, dbUniqueName, dbHostName,
                        getIOFileStatsListFromRS(oraIOFileStatsPreparedStatement.getResultSet())));

                oraIOFileStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error getting io file stats from database "
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    private boolean collectIOFunctionStats(boolean shutdown,long currentDateTime, PreparedStatement oraIOFunctionStatsPreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraIOFunctionStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSIOFUNCTIONSTAT, currentDateTime, dbUniqueName, dbHostName,
                        getIOFunctionStatsListFromRS(oraIOFunctionStatsPreparedStatement.getResultSet())));

                oraIOFunctionStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error getting io function stats from database"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    private boolean collectSystemStats(boolean shutdown,long currentDateTime, PreparedStatement oraSysStatsPreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraSysStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSYSTEMSTAT, currentDateTime, dbUniqueName, dbHostName,
                        getSysStatsListFromRS(oraSysStatsPreparedStatement.getResultSet())));

                oraSysStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing system statistics"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    private boolean collectFilesSize(boolean shutdown,long currentDateTime, PreparedStatement oraFilesSizePreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraFilesSizePreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSFILESSIZE, currentDateTime, dbUniqueName, dbHostName,
                        getFilesSizeListFromRS(oraFilesSizePreparedStatement.getResultSet())));

                oraFilesSizePreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing files size"
                        + "\t" + e.getMessage()
                );
                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    private boolean collectSegmentsSize(boolean shutdown,long currentDateTime,PreparedStatement oraSegmentsSizePreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraSegmentsSizePreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSEGMENTSSIZE, currentDateTime, dbUniqueName, dbHostName,
                        getSegmentsSizeListFromRS(oraSegmentsSizePreparedStatement.getResultSet())));

                oraSegmentsSizePreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing segments size"
                        + "\t" + e.getMessage()
                );
                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    private boolean collectStatNames(boolean shutdown,PreparedStatement oraStatNamesPreparedStatement) {
        try {
            oraStatNamesPreparedStatement.execute();
            ckhQueue.put(new OraCkhMsg(RSSTATNAME, 0, dbUniqueName, null,
                    getStatNamesListFromRS(oraStatNamesPreparedStatement.getResultSet())));

            oraStatNamesPreparedStatement.close();
        } catch (Exception e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error processing statistics names"
                    + "\t" + e.getMessage()
            );

            shutdown = true;
        }
        return shutdown;
    }

    public void RunCollection() throws InterruptedException {
        long currentDateTime;
        boolean shutdown = false;        
        long begints, endts;
        PreparedStatement oraSysStatsPreparedStatement=null;
        PreparedStatement oraStatNamesPreparedStatement=null;
        PreparedStatement oraSegmentsSizePreparedStatement=null;
        PreparedStatement oraFilesSizePreparedStatement=null;
        PreparedStatement oraIOFileStatsPreparedStatement=null;
        PreparedStatement oraIOFunctionStatsPreparedStatement=null;        
        try {
            oraSysStatsPreparedStatement = con.prepareStatement(ORASYSSTATSQUERY);
            oraSysStatsPreparedStatement.setFetchSize(500);
            oraStatNamesPreparedStatement = con.prepareStatement(ORASTATNAMESQUERY);
            oraStatNamesPreparedStatement.setFetchSize(1000);
            oraFilesSizePreparedStatement = con.prepareStatement(ORAFILESSIZEQUERY);
            oraFilesSizePreparedStatement.setFetchSize(100);
            oraSegmentsSizePreparedStatement = con.prepareStatement(ORASEGMENTSSIZEQUERY);
            oraSegmentsSizePreparedStatement.setFetchSize(100);
            //oraIOFileStatsPreparedStatement = con.prepareStatement((dbVersion>=12)? ORAIOFILESTATSQUERYCDB: ORAIOFILESTATSQUERY);
            oraIOFileStatsPreparedStatement = con.prepareStatement(ORAIOFILESTATSQUERY);
            oraIOFileStatsPreparedStatement.setFetchSize(100);
            oraIOFunctionStatsPreparedStatement = con.prepareStatement(ORAIOFUNCTIONSTATSQUERY);
            oraIOFunctionStatsPreparedStatement.setFetchSize(100);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot prepare statements"
                    + "\t" + e.getMessage()
            );
            shutdown = true;
        }
        int snapcounter = 0;
        
        if (dbRole==0){
            shutdown = collectStatNames(shutdown,oraStatNamesPreparedStatement);
        }
        
        while (!shutdown) {
            currentDateTime = Instant.now().getEpochSecond();
            
            begints = System.currentTimeMillis();
            
            shutdown = collectIOFileStats(shutdown,currentDateTime,oraIOFileStatsPreparedStatement);
            
            shutdown = collectIOFunctionStats(shutdown,currentDateTime,oraIOFunctionStatsPreparedStatement);
            
            if(dbRole==0 && (snapcounter ==0 || (snapcounter>=12 && snapcounter % 12 == 0)) ){
                shutdown = collectSystemStats(shutdown,currentDateTime,oraSysStatsPreparedStatement);
            }
            
            if ( dbRole==0 && (snapcounter==0 || snapcounter == 60) ) {
                shutdown = collectFilesSize(shutdown,currentDateTime,oraFilesSizePreparedStatement);
                shutdown = collectSegmentsSize(shutdown,currentDateTime,oraSegmentsSizePreparedStatement);
                if(snapcounter>0){
                    snapcounter = 0;
                }
            } 
            
            endts = System.currentTimeMillis();
            
            if (endts - begints < SECONDSBETWEENSYSSTATSSNAPS * 1000L) {
                TimeUnit.SECONDS.sleep(SECONDSBETWEENSYSSTATSSNAPS - (int) ((endts - begints) / 1000L));
            }
            
            snapcounter++;
        }
        cleanup(
                oraSegmentsSizePreparedStatement,
                oraFilesSizePreparedStatement,
                oraSysStatsPreparedStatement,
                oraIOFunctionStatsPreparedStatement,
                oraIOFileStatsPreparedStatement
        );
    }
}
