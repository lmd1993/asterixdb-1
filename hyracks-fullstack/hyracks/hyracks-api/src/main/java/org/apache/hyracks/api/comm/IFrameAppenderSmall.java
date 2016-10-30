package org.apache.hyracks.api.comm;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
/**
 * Created by MingdaLi on 10/26/16.
 */
public interface IFrameAppenderSmall extends IFrameAppender {
    void reset(IFrame frame, boolean clear) throws HyracksDataException;

    /**
     * Get how many tuples in current frame.
     *
     * @return
     */
    int getTupleCount();

    /**
     * Get the ByteBuffer which contains the frame data.
     *
     * @return
     */
    ByteBuffer getBuffer();

    /**
     * Write the frame content to the given writer.
     * Clear the inner buffer after write if {@code clear} is <code>true</code>.
     *
     * @param outWriter the output writer
     * @param clear     indicate whether to clear the inside frame after writing or not.
     * @throws HyracksDataException
     */
    void write(IFrameWriter outWriter, boolean clear,int length) throws HyracksDataException;

    /**
     * Write currently buffered records to {@code writer} then flushes {@code writer}. The inside frame is always cleared
     * @param writer the FrameWriter to write to and flush
     * @throws HyracksDataException
     */
    public default void flush(IFrameWriter writer) throws HyracksDataException {
        write(writer, true);
        writer.flush();
    }

}
