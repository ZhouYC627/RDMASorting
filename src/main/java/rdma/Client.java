package rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;


public class Client implements RdmaEndpointFactory<ClientEndpoint> {
    private RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public ClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new ClientEndpoint(endpointGroup, idPriv, serverSide);

    }

    public void run() throws Exception {
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);

        ClientEndpoint endpoint = endpointGroup.createEndpoint();


        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);

        endpoint.connect(address, RdmaConfigs.TIMEOUT);
        InetSocketAddress _addr = (InetSocketAddress) endpoint.getDstAddr();
        DiSNILogger.getLogger().info("client connected, address " + _addr.toString());

        // prepare a message to send to the mapper about the data location that it writes
        ByteBuffer sendBuffer = endpoint.getSendBuf();
        IbvMr dataMemoryRegion = endpoint.getDataMr();

        // send memory address, length and lkey to the server
        sendBuffer.putLong(dataMemoryRegion.getAddr());
        sendBuffer.putInt(dataMemoryRegion.getLength());
        sendBuffer.putInt(dataMemoryRegion.getLkey());
        sendBuffer.putInt(3);
        sendBuffer.putInt(2);
        sendBuffer.clear();

        DiSNILogger.getLogger().info("rdma.Client::sending message");
        endpoint.postSend(endpoint.getWrList_send()).execute().free();

        // wait until the RDMA SEND message to be sent
        endpoint.getWcEvents().take();
        DiSNILogger.getLogger().info("Sending completed");

        // wait for the receive buffer received immediate value
        endpoint.getWcEvents().take();
        DiSNILogger.getLogger().info("rdma.Client::Write Completed notified by the immediate value");

        // build a RR to receive final message
//        IbvRecvWR recvWR = endpoint.getRecvWR();
//        recvWR.setWr_id(2002);
//        endpoint.postRecv(endpoint.getWrList_recv());


        ByteBuffer dataBuf = endpoint.getDataBuf();
        DiSNILogger.getLogger().info("rdma.Client::memory is written by server: " + dataBuf.asCharBuffer().toString());

//        DiSNILogger.getLogger().info("rdma.Client::final message received");

        //close everything
        endpoint.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception {
        CmdLineCommon cmdLine = new CmdLineCommon("rdma.Client");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        host = cmdLine.getIp();
        port = cmdLine.getPort();

        this.run();
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Client simpleClient = new Client();
        simpleClient.launch(args);
    }

}