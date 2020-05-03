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

    //TODO only for testing
    private int testing_mapperId = 0;
    private int testing_reducerId = 100;

    @Override
    public ClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new ClientEndpoint(endpointGroup, idPriv, serverSide);

    }

    public void run() throws Exception {
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 8, 128);
        endpointGroup.init(this);

        ClientEndpoint endpoint = endpointGroup.createEndpoint();


        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);

        endpoint.connect(address, RdmaConfigs.TIMEOUT);
        InetSocketAddress _addr = (InetSocketAddress) endpoint.getDstAddr();
        DiSNILogger.getLogger().info("client connected, address " + _addr.toString());

        // send memory address, length and lkey to the server
        for (int i = 0; i < 10; i++) {
            ByteBuffer sendBuffer = endpoint.getSendBuf();
            IbvMr dataMemoryRegion = endpoint.getDataMr();
            sendBuffer.clear();
            sendBuffer.putLong(dataMemoryRegion.getAddr());
            sendBuffer.putInt(dataMemoryRegion.getLkey());
            sendBuffer.putInt(testing_mapperId++);
            sendBuffer.putInt(testing_reducerId++);
            sendBuffer.clear();

            endpoint.executePostSend();

            // wait until the RDMA SEND message to be sent
            IbvWC sendWc = endpoint.getSendCompletionEvents().take();
            DiSNILogger.getLogger().info("Send wr_id: " + sendWc.getWr_id() + " op: " + sendWc.getOpcode());
            DiSNILogger.getLogger().info("Sending" + i +" completed");

            // wait for the receive buffer received immediate value
            IbvWC recWc = endpoint.getWriteCompletionEvents().take();
            DiSNILogger.getLogger().info("wr_id: " + recWc.getWr_id() + " op: " + recWc.getOpcode());
            endpoint.executePostRecv();
            ByteBuffer dataBuf = endpoint.getDataBuf();

            DiSNILogger.getLogger().info("rdma.Client::Write" + i +" Completed notified by the immediate value");
            dataBuf.clear();
            //DiSNILogger.getLogger().info("rdma.Client::memory is written by server: " + dataBuf.asCharBuffer().toString());
            int length = 0;
            byte[] byteArray = new byte[RdmaConfigs.LOAD_SIZE];
            while (length < dataBuf.limit()) {
                byte b = dataBuf.get();
                if (b == 0) {
                    break;
                }
                byteArray[length++] = b;
            }
            DiSNILogger.getLogger().info("rdma.Client::memory is written by server: " + new String(byteArray, 0, length));

        }

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