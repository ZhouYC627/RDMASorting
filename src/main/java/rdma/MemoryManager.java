package rdma;

import com.ibm.disni.verbs.IbvMr;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryManager {
    private final ArrayBlockingQueue<MemoryInfo> freeMemoryBlocks;
    private final Map<Integer, MemoryInfo> usedMemoryBlocks;

    public MemoryManager(ByteBuffer[] totalMemoryBlocks) {
        this.usedMemoryBlocks = new ConcurrentHashMap<>();
        this.freeMemoryBlocks = new ArrayBlockingQueue<>(RdmaConfigs.TOTAL_MEMORY_BLOCK);

        int idx = 0;
        for (ByteBuffer buffer : totalMemoryBlocks) {
            freeMemoryBlocks.add(new MemoryInfo(idx++, buffer));
        }

    }

    public MemoryInfo getFreeMemory() throws InterruptedException {
        MemoryInfo freeMemory = freeMemoryBlocks.take();
        usedMemoryBlocks.put(freeMemory.getIndex(), freeMemory);
        return freeMemory;
    }

    public void releaseMemory(MemoryInfo memoryInfo) {
        MemoryInfo releasedMemory = usedMemoryBlocks.remove(memoryInfo.getIndex());
        releasedMemory.getByteBuffer().clear();
        freeMemoryBlocks.add(releasedMemory);
    }
}
