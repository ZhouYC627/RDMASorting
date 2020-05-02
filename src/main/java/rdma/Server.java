package rdma;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;

import java.net.InetAddress;
import java.net.InetSocketAddress;


public class Server{
    private String host;
    private int port = 1919;

    private void launch(String[] args) throws Exception {
        CmdLineCommon cmdLine = new CmdLineCommon("Server");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        host = cmdLine.getIp();
        port = cmdLine.getPort();

        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress addr = new InetSocketAddress(ipAddress, port);
        ServerConnectionEndpoint conn = new ServerConnectionEndpoint(addr);
        conn.run();

    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Server simpleServer = new Server();
        simpleServer.launch(args);
    }

}
