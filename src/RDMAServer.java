import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class RDMAServer extends Thread {
    private ServerSocket serverSocket;

    public RDMAServer(int port) throws IOException{
        serverSocket = new ServerSocket(port);
        ///serverSocket.setSoTimeout(50000);
    }

    @Override
    public void run(){
        while (true){
            try {
                System.out.println("Waiting for conncetion...");
                System.out.println("Port: " + serverSocket.getLocalPort());

                Socket server = serverSocket.accept();
                System.out.println("Connected to: " + server.getRemoteSocketAddress());

                DataInputStream in = new DataInputStream(server.getInputStream());
                String outputName = in.readUTF();
                System.out.println(outputName);
                ArrayList<IndexRecord> indexList = getIndexList(in);

                MapOutputReader reader = new MapOutputReader(outputName, indexList);

                // Read a 64*1024 bytebuffer to buf
                ByteBuffer buf = ByteBuffer.allocate(reader.BLOCK_SIZE);
                Future<Integer> len = reader.getBlockFuture(0, buf);

                System.out.println(len.get());
                buf.flip();
                for (int i = 0; i < buf.limit(); i++){
                    System.out.print((char)buf.get());
                }
                server.close();
            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<IndexRecord> getIndexList(DataInputStream in) throws IOException {

        int partitions = in.readInt();
        ArrayList<IndexRecord> indexList = new ArrayList<>();

        for (int i = 0; i < partitions; i++){
            long startOffset = in.readLong();
            long rawLength = in.readLong();
            long partLength = in.readLong();
            IndexRecord ir = new IndexRecord(startOffset, rawLength, partLength);
            indexList.add(ir);
        }
        return indexList;
    }
}
