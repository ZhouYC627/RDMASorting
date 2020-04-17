import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class FindFile implements Runnable {
    private String path;
    FindFile(String path){
        this.path = path;
    }

    private boolean getFile(String path){
        File file = new File(path);
        File[] array = file.listFiles();
        assert array != null;
        for (File value : array) {
            if (value.isFile()) {
                System.out.println(value.getName());
                //System.out.println("----" + value);
                //System.out.println("----" + value.getPath());
                if (value.getName().endsWith("rdma")){
                    loadFromIndex(value.getName());
                    return true;
                }
            } else if (value.isDirectory()) {
                getFile(value.getPath());
            }
        }
        return false;
    }
    @Override
    public void run() {
        System.out.print(".");
        getFile(this.path);
    }

    private void loadFromIndex(String indexFileName){
        File indexFile = new File(indexFileName);
        ArrayList<IndexRecord> records = new ArrayList<>();
        try {
            Scanner sc = new Scanner(indexFile);
            long startOffset = sc.nextLong();
            long rawLength = sc.nextLong();
            long partLength = sc.nextLong();
            IndexRecord ir = new IndexRecord(startOffset, rawLength, partLength);
            records.add(ir);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (IndexRecord ir : records){
            System.out.println(ir.toString());
        }
    }
}
