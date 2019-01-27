package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CkhQueueConsumer  extends Thread {

    private OraCkhMsg ckhQueueMessage;
    
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private final ComboPooledDataSource ckhDataSource;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    
    SL4JLogger lg;

    public CkhQueueConsumer(BlockingQueue<OraCkhMsg> queue, ComboPooledDataSource ds) {
        ckhQueue        = queue;
        ckhDataSource   = ds;
    }

    public void run() {
        lg = new SL4JLogger();

        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Starting clickhouse queue consumer " + Thread.currentThread().getName());
        try {
            while(true) {
                ckhQueueMessage = ((OraCkhMsg) ckhQueue.take());
                executor.execute(
                    new StatProcessorCKH(
                        ckhQueueMessage.dataType, 
                        ckhQueueMessage.currentDateTime, 
                        ckhQueueMessage.dbUniqueName, 
                        ckhQueueMessage.dbHostaName, 
                        ckhDataSource, 
                        ckhQueueMessage.dataList
                    )
                );
            }
        } catch (InterruptedException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+ 
                    "Error retrieving message from clickhouse queue"
            );
            e.printStackTrace();
        }
    }
}
