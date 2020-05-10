package rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;


public class Client {
    private RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup;


    public Client() throws IOException {
        ReducerClientEndpointFactory factory = new ReducerClientEndpointFactory();
        endpointGroup = factory.getEndpointGroup();
    }

    public RdmaDataInputStream createRdmaStream(String host, int port, int mapperId, int reducerId) throws Exception {
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        return new RdmaDataInputStream(endpointGroup, address, mapperId, reducerId);
    }


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Client simpleClient = new Client();

        CmdLineCommon cmdLine = new CmdLineCommon("rdma.Client");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        String host = cmdLine.getIp();
        int port = cmdLine.getPort();

        int testing_mapperId = 0;
        int testing_reducerId = 0;
        int test_len = 100;
        RdmaDataInputStream rdmaStream = simpleClient.createRdmaStream(host, port, testing_mapperId, testing_reducerId);

        for (int i = 0; i < 50; i++) {
            byte[] byteArray = new byte[RdmaConfigs.LOAD_SIZE];
            int bytesWritten = rdmaStream.read(byteArray, 0, test_len++);
            DiSNILogger.getLogger().info("ByteArray" + i + ": " + new String(byteArray, 0, bytesWritten));
        }
        rdmaStream.closeEndpoint();
        simpleClient.closeEndpointGroup();
    }

    public void closeEndpointGroup() throws IOException, InterruptedException {
        this.endpointGroup.close();
    }

}