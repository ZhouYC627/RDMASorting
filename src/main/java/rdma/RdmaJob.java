package rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;

import java.io.IOException;
import java.util.concurrent.Future;

public class RdmaJob {
    private Future<Integer> future;
    private MapperEndpoint endpoint;
    private long addr;
    private int key;

    //TODO future should be type Future<Integer>
    public RdmaJob(Future<Integer> future, MapperEndpoint endpoint, long addr, int key) {
        // this.future = future;
        this.future = future;
        this.endpoint = endpoint;
        this.addr = addr;
        this.key = key;
    }

    public boolean isDone() {
        return true;
        //return future.isDone();
    }

    public void sendData() throws InterruptedException, IOException {
        endpoint.executeRDMAWrite(addr, key);

        DiSNILogger.getLogger().info("MapperClient::write memory to server " + endpoint.getDstAddr());
        IbvWC writeWC = endpoint.getWritingCompletionEvents().take();
        //TODO
        DiSNILogger.getLogger().info("Send wr_id: " + writeWC.getWr_id() + " op: " + writeWC.getOpcode());
    }


}
