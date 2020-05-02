package rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

public class RdmaProcess implements Runnable{
    private ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer;

    RdmaProcess(ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer) {
        this.pendingRequestsFromReducer = pendingRequestsFromReducer;

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
                dataBuf = endpoint.getDataBuf();
                dataBuf.asCharBuffer().put("This is Required data of mapperId " + mapperId + " for reducerId " + reducerId);
                //TODO probably need to know the size of the data
                endpoint.executeRDMAWrite(addr, lkey);

                DiSNILogger.getLogger().info("MapperClient::write memory to server " + endpoint.getDstAddr());
                IbvWC writeWC = endpoint.getWritingCompletionEvents().take();
                //TODO
                DiSNILogger.getLogger().info("Send wr_id: " + writeWC.getWr_id() + " op: " + writeWC.getOpcode());


//                endpoint.close();
//                DiSNILogger.getLogger().info("closing endpoint, done");

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

    }
}
