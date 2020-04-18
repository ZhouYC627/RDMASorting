import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;

public class LoadDisk {
    //ExecutorService executor;
    public LoadDisk(){
        //this.executor = Executors.newFixedThreadPool(3);
    }

    public static void main(String[] args){
        /*
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);

        String path = args[0];
        FindFile f = new FindFile(path);
        //FindFile f = new FindFile("/Users/answer/Desktop/531");
        System.out.println("Searching file in " + path);
        executor.scheduleAtFixedRate(f, 1000, 1000, TimeUnit.MILLISECONDS);
         */

        int port = Integer.parseInt(args[0]);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            executor.execute(new RDMAServer(port));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

    }
}
