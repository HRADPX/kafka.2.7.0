package kafka.examples;

import java.nio.ByteBuffer;

import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * @author huangran <huangran@kuaishou.com>
 * Created on 2023-06-05
 */
public class ByteBufferDemo {

    public static void main(String[] args) {

        ByteBuffer buffer = ByteBuffer.allocate(100);
        ByteBufferOutputStream outputStream = new ByteBufferOutputStream(buffer);

        outputStream.position(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
    }
}
