package ifp.at.mikorn.kr;

import any.data.labeled_files;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by nqlong on 16-Apr-18.
 */

public class App {
    private final static String NAME_FILE_LABELED_HDFS = "image_test_map(original_hdfs)";
    private final static String NAME_FILE_LABELED_HDFS2 = "test2";
    private final static String PATH_OF_RESOURCES = "src/main/resources/";

    public static void main(String[] args) {
        // Deserialize Users from disk
        DatumReader<labeled_files> userDatumReader = new SpecificDatumReader<labeled_files>(labeled_files.class);
        File labeledFile = new File(String.format("src/main/resources/hdfs/%s", NAME_FILE_LABELED_HDFS));

        DataFileReader<labeled_files> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<labeled_files>(labeledFile, userDatumReader);
            labeled_files labeled_files = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                labeled_files = dataFileReader.next(labeled_files);
                System.out.println(labeled_files.getRawData()); // ByteBuffer

                // write to file
                ByteBuffer byteBuffer = labeled_files.getRawData();
                // In file
                File outFile = new File(String.format("%s/parse-result/labeled2Out.png", PATH_OF_RESOURCES));
                // append or override the file
                boolean append = false;

                FileChannel channel = new FileOutputStream(outFile, append).getChannel();
                // Flips this buffer. The limit is set to the current position and then
                // the position is set to zero. If the mark is defined then it is discarded
                // byteBuffer.flip();

                // writes a sequence of bytes to this channel from the given buffer
                channel.write(byteBuffer);

                // close the chanel
                channel.close();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
