/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author lesha
 */
public class CkhQueueConsumer extends Thread {

    private static final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private OraCkhMsg ckhQueueMessage;
    private BlockingQueue<OraCkhMsg> ckhQueue;
    private ComboPooledDataSource ckhDataSource;
    private int timeToSleep;
    
    SL4JLogger lg;

    public CkhQueueConsumer(BlockingQueue<OraCkhMsg> queue, ComboPooledDataSource ds, int sleep) {
        ckhQueue = queue;
        ckhDataSource = ds;
        timeToSleep = sleep;
    }

    public void run() {
        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                + "Starting clickhouse queue consumer " + Thread.currentThread().getName()
        );
        while( true ){
            try {
                ckhQueueMessage = ckhQueue.take();
                
            } catch (InterruptedException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Error retrieving message from clickhouse queue"
                );
                e.printStackTrace();
            }
        }
    }
}
