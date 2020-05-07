package rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

public class RdmaProcess implements Runnable{
    private ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer;
    private final ArrayBlockingQueue<RdmaJob> jobQueue;

    RdmaProcess(ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer, ArrayBlockingQueue<RdmaJob> jobQueue) {
        this.pendingRequestsFromReducer = pendingRequestsFromReducer;
        this.jobQueue = jobQueue;

    }

    @Override
    public void run() {
        while (true) {
            try {
                DiSNILogger.getLogger().info("Rdma Process running.... waiting for Requests");
                MapperEndpoint endpoint = pendingRequestsFromReducer.take();

                //TODO remove After testing
                IbvWC recWc = endpoint.getReceiveCompletionEvents().take();
                DiSNILogger.getLogger().info("Recv wr_id: " + recWc.getWr_id() + " op: " + recWc.getOpcode());
                // Post another recv on this endpoint for further messages
                endpoint.executePostRecv();

                ByteBuffer recvBuffer = endpoint.getRecvBuf();
                recvBuffer.clear();
                long addr = recvBuffer.getLong();
                int lkey = recvBuffer.getInt();
                int mapperId = recvBuffer.getInt();
                int reducerId = recvBuffer.getInt();
                recvBuffer.clear();
                DiSNILogger.getLogger().info("information received, mapperId " + mapperId + " for reducerId " + reducerId);

                ByteBuffer dataBuf = endpoint.getDataBuf();
                dataBuf.clear();
                //dataBuf.asCharBuffer().put("This is Required data of mapperId " + mapperId + " for reducerId " + reducerId);
                String msg = "This is Required data of mapperId " + mapperId + " for reducerId " + reducerId;
                byte[] msgArr = msg.getBytes();
                DiSNILogger.getLogger().info("msg len: " + msgArr.length);
                dataBuf.put(msgArr);

                Integer fakeFuture = 1;
                RdmaJob job = new RdmaJob(fakeFuture, endpoint, addr, lkey);
                jobQueue.add(job);


//                endpoint.close();
//                DiSNILogger.getLogger().info("closing endpoint, done");

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

    }
}
