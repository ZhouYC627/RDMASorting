package rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapOutputReader {

    final int BLOCK_SIZE = 64*1024;
    final int NUMBER_OF_THREAD = 3;

    private String outputFileName;
    private ArrayList<IndexRecord> indexRecords;
    private long[] currentPos;
    private FileChannel[] channels;

    private ExecutorService executor;

    public MapOutputReader(String outputFileName, ArrayList<IndexRecord> indexList) throws IOException {
        this.outputFileName = outputFileName;
        this.indexRecords = indexList;
        currentPos = new long[indexList.size()];
        channels = new FileChannel[indexList.size()];

        for (int i = 0; i < indexList.size(); i++){
            currentPos[i] = (indexList.get(i).startOffset);
            channels[i] = FileChannel.open(Paths.get(outputFileName));
            channels[i].position(currentPos[i]);
        }
        executor = Executors.newFixedThreadPool(NUMBER_OF_THREAD);

    }

    public Future<Integer> getBlockFuture(int reduce, ByteBuffer buf){

        //ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE);
        Callable<Integer> readTask = new ReadBlock(channels[reduce], buf);
        Future<Integer> len = executor.submit(readTask);
        currentPos[reduce] += BLOCK_SIZE;
        return len;

    }

    class ReadBlock implements Callable<Integer> {
        FileChannel channel;
        ByteBuffer buffer;

        public ReadBlock(FileChannel channel, ByteBuffer buf) {
            this.channel = channel;
            this.buffer = buf;
        }

        @Override
        public Integer call() throws Exception {

            return channel.read(buffer);
        }
    }

}
