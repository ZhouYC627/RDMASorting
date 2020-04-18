package rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;
import com.ibm.disni.util.DiSNILogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class MapperEndpoint extends RdmaActiveEndpoint {
    private ByteBuffer recvBuffer; // used to receive the memory information from Mapper
    private SVCPostRecv postRecv;
    private ArrayBlockingQueue<IbvWC> writingCompletionEvents;
    private ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer;

    public MapperEndpoint(RdmaActiveEndpointGroup<? extends MapperEndpoint> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(endpointGroup, idPriv, serverSide);
        writingCompletionEvents = new ArrayBlockingQueue<>(100);
    }

    public IbvMr registerMr(ByteBuffer buffer) throws IOException {
        return this.registerMemory(buffer).execute().free().getMr();
    }

    public LinkedList<IbvSendWR> prepareSendWrList(IbvMr mr, int length) {
        LinkedList<IbvSendWR> sendWr_list = new LinkedList<>();
        IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(length);
        sge.setLkey(mr.getLkey());
        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sge);

        IbvSendWR sendWr = new IbvSendWR();
        sendWr.setWr_id(RdmaConfigs.getNextWrID());
        sendWr.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWr.setSg_list(sgeList);
        sendWr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWr_list.add(sendWr);

        return sendWr_list;
    }


    /** RDMA write with immediate operations, however, do notify the remote host of the immediate value
     *  Write with immediate will consume a Receive Request. Optionally, an immediate 4 byte value may be
     *  transmitted with the data buffer. This immediate value is presented to the receiver as part of the
     *  receive notification, and is not contained in the data buffer.
     */

    public LinkedList<IbvSendWR> prepareRdmaWrList(IbvMr mr, int length, int opcode, long remoteAddr, int rkey) {
        LinkedList<IbvSendWR> writeWr_list = new LinkedList<>();
        IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(length);
        sge.setLkey(mr.getLkey());
        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sge);

        IbvSendWR writeWr = new IbvSendWR();
        writeWr.setWr_id(RdmaConfigs.getNextWrID());
        writeWr.setOpcode(opcode);
        writeWr.setSg_list(sgeList);
        writeWr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        writeWr.getRdma().setRemote_addr(remoteAddr);
        writeWr.getRdma().setRkey(rkey);
        writeWr_list.add(writeWr);

        return writeWr_list;
    }

    public LinkedList<IbvRecvWR> prepareRecvWrList(IbvMr mr, int length) {
        LinkedList<IbvRecvWR> recvWr_list = new LinkedList<>();
        IbvSge recvSge = new IbvSge();
        recvSge.setAddr(mr.getAddr());
        recvSge.setLength(length);
        recvSge.setLkey(mr.getLkey());
        LinkedList<IbvSge> sgeList_recv = new LinkedList<>();
        sgeList_recv.add(recvSge);

        IbvRecvWR recvWr = new IbvRecvWR();
        recvWr.setWr_id(RdmaConfigs.getNextWrID());
        recvWr.setSg_list(sgeList_recv);
        recvWr_list.add(recvWr);

        return recvWr_list;
    }



    @Override
    public void init() throws IOException {
        super.init();

        this.recvBuffer = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);
        IbvMr recvMr = registerMr(recvBuffer);
        LinkedList<IbvRecvWR> recvWr_list = prepareRecvWrList(recvMr, recvMr.getLength());
        this.postRecv = postRecv(recvWr_list);
        this.postRecv.execute();

        DiSNILogger.getLogger().info("Init PostRecv");

    }

    public void initReceiving(ArrayBlockingQueue<MapperEndpoint> pendingRequestsFromReducer) throws IOException {
        this.pendingRequestsFromReducer = pendingRequestsFromReducer;
        DiSNILogger.getLogger().info("Init RequestQueue");
    }

    @Override
    public void dispatchCqEvent(IbvWC wc){
        if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)) {
            DiSNILogger.getLogger().info("Recv Completion WR in RequestQueue!");
            pendingRequestsFromReducer.add(this);
        } else{
            DiSNILogger.getLogger().info("Writing Completed!");
            writingCompletionEvents.add(wc); // this wc events for send events
        }
    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        return writingCompletionEvents;
    }

    public ByteBuffer getRecvBuf() {
        return recvBuffer;
    }

    public SVCPostRecv getPostRecv() {
        return this.postRecv;
    }
}
