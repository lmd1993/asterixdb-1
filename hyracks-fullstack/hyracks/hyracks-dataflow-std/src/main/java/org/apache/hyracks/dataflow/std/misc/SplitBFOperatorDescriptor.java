package org.apache.hyracks.dataflow.std.misc;

/**
 * Created by MingdaLi on 9/29/16.
 */
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
public class SplitBFOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final static int SPLITTER_MATERIALIZER_ACTIVITY_ID = 0;
    private final static int MATERIALIZE_READER_ACTIVITY_ID = 1;

    private final boolean[] outputMaterializationFlags;
    private final boolean requiresMaterialization;
    private final int numberOfNonMaterializedOutputs;
    private final int numberOfMaterializedOutputs;
    private final int[] BFKeys;
    private final int countAll;//Estimination of the number of items


    public SplitBFOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,int[] BFKeys, int countAll) {
        this(spec, rDesc, outputArity, new boolean[outputArity], BFKeys,countAll);
    }

    public SplitBFOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,
                                     boolean[] outputMaterializationFlags, int BFKeys[], int countAll) {
        super(spec, 1, outputArity);
        for (int i = 0; i < outputArity; i++) {
            recordDescriptors[i] = rDesc;
        }
        this.outputMaterializationFlags = outputMaterializationFlags;
        boolean reqMaterialization = false;
        int matOutputs = 0;
        int nonMatOutputs = 0;
        for (boolean flag : outputMaterializationFlags) {
            if (flag) {
                reqMaterialization = true;
                matOutputs++;
            } else {
                nonMatOutputs++;
            }
        }

        this.requiresMaterialization = reqMaterialization;
        this.numberOfMaterializedOutputs = matOutputs;
        this.numberOfNonMaterializedOutputs = nonMatOutputs;
        this.BFKeys=BFKeys;
        this.countAll=countAll;


    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SplitterMaterializerActivityNode sma =
                new SplitterMaterializerActivityNode(new ActivityId(odId, SPLITTER_MATERIALIZER_ACTIVITY_ID));
        builder.addActivity(this, sma);
        builder.addSourceEdge(0, sma, 0);
        int pipelineOutputIndex = 0;
        int activityId = MATERIALIZE_READER_ACTIVITY_ID;
        for (int i = 0; i < outputArity; i++) {
            if (outputMaterializationFlags[i]) {
                MaterializeReaderActivityNode mra =
                        new MaterializeReaderActivityNode(new ActivityId(odId, activityId++));
                builder.addActivity(this, mra);
                builder.addBlockingEdge(sma, mra);
                builder.addTargetEdge(i, mra, 0);
            } else {
                builder.addTargetEdge(i, sma, pipelineOutputIndex++);
            }
        }
    }

    private final class SplitterMaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SplitterMaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                                                       IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputOperatorNodePushable() {
                private MaterializerTaskState state;
                private FrameTupleAccessor accessorBuild;
                private final IFrameWriter[] writers = new IFrameWriter[numberOfNonMaterializedOutputs];
                private final boolean[] isOpen = new boolean[numberOfNonMaterializedOutputs];
                double falsePositiveProbability=0.1;//Set by myself for BloomFilter
                BloomFilter<String> BF=new BloomFilter<String>(falsePositiveProbability,countAll);

                @Override
                public void open() throws HyracksDataException {
                    //BloomFilter bf = new BloomFilter(bufferCache, harness.getFileMapProvider(), harness.getFileReference(),keyFields);

                    accessorBuild=new FrameTupleAccessor(recordDescriptors[0]);
                    if (requiresMaterialization) {
                        state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                                new TaskId(getActivityId(), partition), numberOfMaterializedOutputs);
                        state.open(ctx);
                    }
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                        isOpen[i] = true;
                        writers[i].open();
                    }
                }

                @Override
                public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {
                    if (requiresMaterialization) {
                        state.appendFrame(bufferAccessor);

                        bufferAccessor.clear();
                    }
                    accessorBuild.reset(bufferAccessor);
                    byte[] collection=accessorBuild.getBuffer().array();
                    int tupleCount = accessorBuild.getTupleCount();
                    for (int k=0; k<tupleCount;k++){
                        int startOffset=accessorBuild.getTupleStartOffset(k);
                        int fIdx = BFKeys[0];
                        int fStart = accessorBuild.getFieldStartOffset(k, fIdx);
                        int fEnd = accessorBuild.getFieldEndOffset(k, fIdx);
                        int slotlength=accessorBuild.getFieldSlotsLength();
                        byte[] newStr= Arrays.copyOfRange(collection, startOffset+slotlength+fStart+1, startOffset+slotlength+fEnd);
                        String fieldSelf="";
                        try {
                            fieldSelf = new String(newStr, "US-ASCII");
                        }catch(Exception e){
                            System.out.print(e);
                        }
                        BF.add(fieldSelf);//add to bloomfilter

                        System.out.print(fieldSelf);
                    }
                    try {
                    //Convert to byteBuffer
                        ByteBuffer tempForBF = ByteBuffer.wrap(new Serializer().serialize(BF));
                        FrameUtils.flushFrame(tempForBF,writers[1]);
                    }catch(Exception e){
                        System.out.println(e);
                    }

                    FrameUtils.flushFrame(bufferAccessor, writers[0]);


                }

                @Override
                public void flush() throws HyracksDataException {
                    if (!requiresMaterialization) {
                        for (IFrameWriter writer : writers) {
                            writer.flush();
                        }
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    HyracksDataException hde = null;
                    try {
                        if (requiresMaterialization) {
                            state.close();
                            ctx.setStateObject(state);
                        }
                    } finally {
                        for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                            if (isOpen[i]) {
                                try {
                                    writers[i].close();
                                } catch (Throwable th) {
                                    if (hde == null) {
                                        hde = new HyracksDataException(th);
                                    } else {
                                        hde.addSuppressed(th);
                                    }
                                }
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
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                        if (isOpen[i]) {
                            try {
                                writers[i].fail();
                            } catch (Throwable th) {
                                if (hde == null) {
                                    hde = new HyracksDataException(th);
                                } else {
                                    hde.addSuppressed(th);
                                }
                            }
                        }
                    }
                    if (hde != null) {
                        throw hde;
                    }
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    writers[index] = writer;
                }
            };
        }


    }
//The following is working for materilizing work.
    private final class MaterializeReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializeReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                                                       final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    MaterializerTaskState state = (MaterializerTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), SPLITTER_MATERIALIZER_ACTIVITY_ID), partition));
                    state.writeOut(writer, new VSizeFrame(ctx));
                }

            };
        }
    }
}

