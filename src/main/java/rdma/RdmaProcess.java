package rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSendWR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class RdmaProcess implements Runnable {
    private ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer;
    private final MemoryManager memoryManager;
    //TODO MAP<mapId, MapOutputReader> map

    RdmaProcess(ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer) {
        this.pendingRequestsFromReducer = pendingRequestsFromReducer;
        ByteBuffer[] totalMemoryBlocks = new ByteBuffer[RdmaConfigs.TOTAL_MEMORY_BLOCK];
        for (int i = 0; i < totalMemoryBlocks.length; i++) {
            totalMemoryBlocks[i] = ByteBuffer.allocateDirect(RdmaConfigs.LOAD_SIZE);
        }
        memoryManager = new MemoryManager(totalMemoryBlocks);
        DiSNILogger.getLogger().info("Initiating Memory Manager");

        //TODO LoaderServer
        //TODO FutureHandler
    }


    @Override
    public void run() {
        while (true) {
            try {
                DiSNILogger.getLogger().info("Rdma Process running.... waiting for Requests");
                MapperEndpoint endpoint = pendingRequestsFromReducer.take();
                endpoint.getPostRecv().execute();
                DiSNILogger.getLogger().info("Request received! at the endpoint from" + endpoint.getDstAddr());

                // wait the for completion of receiving memory address of the reducer
                // Now the memory information is stored in the receive buffer
                ByteBuffer recvBuffer = endpoint.getRecvBuf();
                recvBuffer.clear();
                long addr = recvBuffer.getLong();
                int length = recvBuffer.getInt();
                int rkey = recvBuffer.getInt();
                int mapperId = recvBuffer.getInt();
                int reducerId = recvBuffer.getInt();
                recvBuffer.clear();
                DiSNILogger.getLogger().info("receiving rdma information, addr " + addr + ", length " + length + ", key " + rkey);
                DiSNILogger.getLogger().info("waking up a loading thread to get mapperId " + mapperId + " for reducerId " + reducerId);
                //TODO check if the reader has been processed for this mapperId

                MemoryInfo freeMemory = memoryManager.getFreeMemory();
                DiSNILogger.getLogger().info("Using memory index" + freeMemory.getIndex());
                ByteBuffer sendBuf = freeMemory.getByteBuffer();

                //TODO get(reducerId, sendBuf) return Future

                IbvMr writeMr = endpoint.registerMr(sendBuf);
                sendBuf.asCharBuffer().put("This is Required data for reducer from Mapper");

                LinkedList<IbvSendWR> writeWr_list = endpoint.prepareRdmaWrList(writeMr, writeMr.getLength(), IbvSendWR.IBV_WR_RDMA_WRITE_WITH_IMM, addr, rkey);

                DiSNILogger.getLogger().info("MapperClient::write memory to server");
                endpoint.postSend(writeWr_list).execute().free();

                // poll the write completion event
                endpoint.getWcEvents().take();
                endpoint.deregisterMemory(writeMr);
                memoryManager.releaseMemory(freeMemory);


                //let's prepare a final message to signal everything went fine
//                ByteBuffer buf = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);
//                IbvMr sendMr = endpoint.registerMr(buf);
//                LinkedList<IbvSendWR> send_list = endpoint.prepareSendWrList(sendMr, writeMr.getLength());
//                DiSNILogger.getLogger().info("MapperClient::Sending final message");
//                endpoint.postSend(send_list).execute().free();
//                endpoint.getWcEvents().take();

//                DiSNILogger.getLogger().info("closing endpoint");
//                endpoint.close();
//                DiSNILogger.getLogger().info("closing endpoint, done");

                Thread.sleep(1000);

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

    }
}
