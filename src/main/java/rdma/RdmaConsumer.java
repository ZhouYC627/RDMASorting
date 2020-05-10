package rdma;

import com.ibm.disni.util.DiSNILogger;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

public class RdmaConsumer implements Runnable {
    private final ArrayBlockingQueue<RdmaJob> jobQueue;

    public RdmaConsumer(ArrayBlockingQueue<RdmaJob> jobQueue) {
        this.jobQueue = jobQueue;
    }

    @Override
    public void run() {
        DiSNILogger.getLogger().info("Rdma Consumer running....");
        while (true) {
            try {
                //DiSNILogger.getLogger().info("Waiting for jobs");
                RdmaJob job = jobQueue.take();
                //DiSNILogger.getLogger().info("Found Job");
                if (job.isDone()) {
                    //DiSNILogger.getLogger().info("Job finished and perform RDMA Write");
                    job.sendData();
                }else {
                    //DiSNILogger.getLogger().info("Job not finished and check next Job");
                    jobQueue.add(job);
                }

            } catch (InterruptedException | IOException | ExecutionException e) {
                e.printStackTrace();
            }

        }
    }
}
