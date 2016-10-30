package org.apache.hyracks.dataflow.std.misc;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.util.BloomFilter;
import org.apache.hyracks.dataflow.std.connectors.Serializer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.dataflow.std.util.BloomFilter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.util.BloomFilter;
import org.apache.hyracks.dataflow.std.connectors.Serializer;

/**
 * Created by MingdaLi on 10/14/16.
 */
public class mergeBFOperatorDescriptor extends AbstractOperatorDescriptor {
    private final static int Merge_BF_ACTIVITY_ID = 0;
    public mergeBFOperatorDescriptor(IOperatorDescriptorRegistry spec){
        super(spec,1,1);
    }
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MergeBFActivityNode mbf=new MergeBFActivityNode(new ActivityId(odId,Merge_BF_ACTIVITY_ID));
        builder.addActivity(this,mbf);
        builder.addSourceEdge(0,mbf,0);
        builder.addTargetEdge(0,mbf,0);

    }

    private final class MergeBFActivityNode extends AbstractActivityNode {

        public MergeBFActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime (final IHyracksTaskContext ctx,IRecordDescriptorProvider recordDescProvider,
                                                             final int partition,
                                                             int nPartitions)
        {

            return new AbstractUnaryInputOperatorNodePushable() {

                private MaterializerTaskState state;
                private FrameTupleAccessor accessorBuild;
                private IFrameWriter writer;
                private boolean isOpen;
                @Override
                public void open() throws HyracksDataException {
                    //BloomFilter bf = new BloomFilter(bufferCache, harness.getFileMapProvider(), harness.getFileReference(),keyFields);

                    accessorBuild=new FrameTupleAccessor(recordDescriptors[0]);

                    isOpen= true;
                    writer.open();

                }

                @Override
                public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {

                    accessorBuild.reset(bufferAccessor);
                    byte[] collection=accessorBuild.getBuffer().array();
                    int tupleCount = accessorBuild.getTupleCount();
                    for (int k=0; k<tupleCount;k++){
                        int startOffset=accessorBuild.getTupleStartOffset(k);

                        int slotlength=accessorBuild.getFieldSlotsLength();
                        String fieldSelf="";
                        try {
                            //fieldSelf = new String(newStr, "US-ASCII");
                        }catch(Exception e){
                            System.out.print(e);
                        }

                        //System.out.print(fieldSelf);
                    }
                    try {
                        //Convert to byteBuffer
                        //ByteBuffer tempForBF = ByteBuffer.wrap(new Serializer().serialize(BF));
                        //FrameUtils.flushFrame(tempForBF,writer);
                    }catch(Exception e){
                        System.out.println(e);
                    }

                    FrameUtils.flushFrame(bufferAccessor, writer);


                }

                @Override
                public void flush() throws HyracksDataException {

                }

                @Override
                public void close() throws HyracksDataException {
                    HyracksDataException hde = null;

                            if (isOpen) {
                                try {
                                    writer.close();
                                } catch (Throwable th) {
                                    if (hde == null) {
                                        hde = new HyracksDataException(th);
                                    } else {
                                        hde.addSuppressed(th);
                                    }
                                }
                            }

                    if (hde != null) {
                        throw hde;
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    HyracksDataException hde = null;

                        if (isOpen) {
                            try {
                                writer.fail();
                            } catch (Throwable th) {
                                if (hde == null) {
                                    hde = new HyracksDataException(th);
                                } else {
                                    hde.addSuppressed(th);
                                }
                            }
                        }

                    if (hde != null) {
                        throw hde;
                    }
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writr, RecordDescriptor recordDesc) {

                }


            };


        }


    }


}
