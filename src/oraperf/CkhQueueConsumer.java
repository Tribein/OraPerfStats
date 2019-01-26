package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CkhQueueConsumer
        extends Thread {

    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private OraCkhMsg ckhQueueMessage;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private final ComboPooledDataSource ckhDataSource;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    SL4JLogger lg;

    public CkhQueueConsumer(BlockingQueue<OraCkhMsg> queue, ComboPooledDataSource ds) {
        this.ckhQueue = queue;
        this.ckhDataSource = ds;
    }

    public void run() {
        this.lg = new SL4JLogger();

        this.lg.LogWarn(this.DATEFORMAT.format(LocalDateTime.now()) + "\t"+"Starting clickhouse queue consumer "
                + Thread.currentThread().getName());
        try {
            for (;;) {
                this.ckhQueueMessage = ((OraCkhMsg) this.ckhQueue.take());

                this.executor.execute(new StatProcessorCKH(this.ckhQueueMessage.dataType, this.ckhQueueMessage.currentDateTime, this.ckhQueueMessage.dbUniqueName, this.ckhQueueMessage.dbHostaName, this.ckhDataSource, this.ckhQueueMessage.dataList));
            }
        } catch (InterruptedException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t"+ "Error retrieving message from clickhouse queue");

            e.printStackTrace();
        }
    }
}
