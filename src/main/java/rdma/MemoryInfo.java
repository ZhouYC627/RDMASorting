package rdma;

import java.nio.ByteBuffer;

public class MemoryInfo {
    private int index;
    private ByteBuffer buffer;

    public MemoryInfo(int index, ByteBuffer buffer) {
        this.index = index;
        this.buffer = buffer;
    }

    public int getIndex() {
        return this.index;
    }

    public ByteBuffer getByteBuffer() {
        return this.buffer;
    }

}
