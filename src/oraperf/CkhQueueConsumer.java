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
public class CkhQueueConsumer {
    private BlockingQueue<OraCkhMsg> ckhQueue;
    ComboPooledDataSource ckhDataSource;
    
    public CkhQueueConsumer(BlockingQueue<OraCkhMsg> queue, ComboPooledDataSource ds){
        ckhQueue = queue;
        ckhDataSource = ds;
    }
}
