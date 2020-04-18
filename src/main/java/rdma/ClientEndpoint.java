package rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class ClientEndpoint extends RdmaActiveEndpoint {
    private ByteBuffer dataBuffer; // used receive the data that read from Mapper
    private ByteBuffer sendBuffer; // used to contain file information that send to Mapper
    private ByteBuffer recvBuffer; // used to receive the memory information from Mapper

    private IbvMr dataMr;
    private IbvMr sendMr;
    private IbvMr recvMr;

    private IbvSendWR writeWR;
    private IbvSendWR sendWR;
    private IbvRecvWR recvWR;

    private LinkedList<IbvSendWR> sendWR_list;
    private LinkedList<IbvRecvWR> recvWR_list;
    private LinkedList<IbvSendWR> writeWR_list;

    // scattered/gathered element
    private IbvSge sgeWrite;
    private LinkedList<IbvSge> sgeList_write;

    private IbvSge sgeSend;
    private LinkedList<IbvSge> sgeList_send;

    private IbvSge sgeRecv;
    private LinkedList<IbvSge> sgeList_recv;

    private ArrayBlockingQueue<IbvWC> workCompletionEvents;


    public ClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(endpointGroup, idPriv, serverSide);

        this.dataBuffer = ByteBuffer.allocateDirect(RdmaConfigs.LOAD_SIZE);
        this.sendBuffer = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);
        this.recvBuffer = ByteBuffer.allocateDirect(RdmaConfigs.SEND_RECV_SIZE);

        this.sendWR = new IbvSendWR();
        this.recvWR = new IbvRecvWR();
        this.writeWR =  new IbvSendWR();

        this.sendWR_list = new LinkedList<>();
        this.recvWR_list = new LinkedList<>();
        this.writeWR_list = new LinkedList<>();

        this.sgeSend = new IbvSge();
        this.sgeRecv = new IbvSge();
        this.sgeWrite = new IbvSge();

        this.sgeList_send = new LinkedList<>();
        this.sgeList_recv = new LinkedList<>();
        this.sgeList_write = new LinkedList<>();

        workCompletionEvents = new ArrayBlockingQueue<>(100);
    }


    @Override
    public void init() throws IOException {
        super.init();

        // register the memory regions of the data buffer
        dataMr = registerMemory(dataBuffer).execute().free().getMr();
        sendMr = registerMemory(sendBuffer).execute().free().getMr();
        recvMr = registerMemory(recvBuffer).execute().free().getMr();

        // init a SEND request
        sgeSend.setAddr(sendMr.getAddr());
        sgeSend.setLength(sendMr.getLength());
        sgeSend.setLkey(sendMr.getLkey());
        sgeList_send.add(sgeSend);

        sendWR.setWr_id(2001);
        sendWR.setSg_list(sgeList_send);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR_list.add(sendWR);

        // init a RECEIVE request
        sgeRecv.setAddr(recvMr.getAddr());
        sgeRecv.setLength(recvMr.getLength());
        sgeRecv.setLkey(recvMr.getLkey());
        sgeList_recv.add(sgeRecv);

        recvWR.setWr_id(2002);
        recvWR.setSg_list(sgeList_recv);
        recvWR_list.add(recvWR);

        postRecv(recvWR_list).execute().free();

    }

    @Override
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        workCompletionEvents.add(wc);
    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        return workCompletionEvents;
    }

    public LinkedList<IbvSendWR> getWrList_send() {
        return sendWR_list;
    }

    public LinkedList<IbvRecvWR> getWrList_recv() {
        return recvWR_list;
    }

    public LinkedList<IbvSendWR> getWrList_Write() {
        return writeWR_list;
    }

    public ByteBuffer getDataBuf() {
        return dataBuffer;
    }

    public ByteBuffer getSendBuf() {
        return sendBuffer;
    }

    public ByteBuffer getRecvBuf() {
        return recvBuffer;
    }

    public IbvSendWR getSendWR() { return sendWR; }

    public IbvRecvWR getRecvWR() {
        return recvWR;
    }

    public IbvSendWR getWriteWR() {
        return writeWR;
    }

    public IbvMr getDataMr() {
        return dataMr;
    }

    public IbvMr getSendMr() {
        return sendMr;
    }
}
