/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author lesha
 */
public class CkhQueueConsumer extends Thread {
    private BlockingQueue<OraCkhMsg> ckhQueue;
    private ComboPooledDataSource ckhDataSource;
    private int timeToSleep;
    
    public CkhQueueConsumer(BlockingQueue<OraCkhMsg> queue, ComboPooledDataSource ds, int sleep){
        ckhQueue = queue;
        ckhDataSource = ds;
        timeToSleep = sleep;
    }
    public void run(){
        
    }
}
