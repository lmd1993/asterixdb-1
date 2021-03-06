package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.*;

import java.io.*;
import java.util.Random;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.RepartitionComputerFactory;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.*;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

import java.nio.ByteBuffer;

/**
 * Created by MingdaLi on 6/16/16.
 */




public class SampleForMultipleJoinOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int outputLimit;//reservior size
    private final int[] keys0; //keys to sample
    private final IBinaryHashFunctionFactory[] hashFunctionFactories; // The hash function for binary of the keys to sample
    private final int statePartitions;//hashfunction will use this to hash

    public SampleForMultipleJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputLimit, int[] keys0, IBinaryHashFunctionFactory[] hashFunctionFactories, int statePartitions) {
        super(spec, 1, 1);
        recordDescriptors[0] = rDesc;
        this.outputLimit = outputLimit;
        this.keys0=keys0;
        this.hashFunctionFactories = hashFunctionFactories;
        this.statePartitions=statePartitions;
    }


    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                                                   final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        //final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private FrameTupleAccessor fta;
            private int currentSize;
            private boolean finished;
            private final ITuplePartitionComputer hpcBuild = new FieldHashPartitionComputerFactory(keys0,
                    hashFunctionFactories).createPartitioner();
            //private final FrameTupleAccessor accessorBuild = new FrameTupleAccessor(rd1);

            @Override
            public void open() throws HyracksDataException {
                fta = new FrameTupleAccessor(recordDescriptors[0]);
                currentSize = 0;
                finished = false;
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (!finished) {
                    fta.reset(buffer);
                    int count = fta.getTupleCount();
                    FrameTupleAppender partialAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    int[][] reservior=new int[statePartitions][outputLimit+1];//store indexes for sample
                    if(ctx.getSharedObject()!=null){
                        Object reserviorObject=ctx.getSharedObject();
                        reservior= (int[][]) reserviorObject;//store indexes for sample
                    }


                    int[] countForEachPartition=new int[statePartitions];
                    // System.out.println("number in each pool");
                    for (int i=0;i<statePartitions;++i){
                        countForEachPartition[i]=reservior[i][outputLimit];
                        //System.out.println(countForEachPartition[i]);
                    }
                    for (int i = 0; i < count; ++i) {
                        int startOffset=fta.getTupleStartOffset(i);
                        //while ( ((startOffset=fta.getTupleStartOffset(i))!=0 )||(i==0)){
                        int entry;
                        //       entry = hpcBuild.partition(fta, i, statePartitions);
                        //get hash value
                        IBinaryHashFunction[] hashFunctions = new IBinaryHashFunction[hashFunctionFactories.length];
                        for (int j = 0; j < hashFunctionFactories.length; ++j) {
                            hashFunctions[j] = hashFunctionFactories[j].createBinaryHashFunction();
                        }
                        int h = 0;
                        int slotLength = fta.getFieldSlotsLength();
                        for (int j = 0; j < keys0.length; ++j) {
                            int fIdx = keys0[j];
                            IBinaryHashFunction hashFn = hashFunctions[j];
                            int fStart = fta.getFieldStartOffset(i, fIdx);
                            int fEnd = fta.getFieldEndOffset(i, fIdx);
                            int length= fEnd-fStart-1;//The length of int
                            int startOfInt=startOffset + slotLength + fStart+1;//The start position of int's first binary
                            int fh=0;
                            for(int g=0;g<length;g++){
                                int a=fta.getBuffer().get(startOfInt);
                                fh=fh*10+a-48;
                                startOfInt++;
                            }

                            h = h * 31 + fh;
                        }
                        if (h < 0) {
                            h = -(h + 1);
                        }
                        entry=h%statePartitions;

                        if(countForEachPartition[entry]<outputLimit){
                            reservior[entry][countForEachPartition[entry]]=h;
                            countForEachPartition[entry]++;
                        }else{
                            countForEachPartition[entry]++;
                            Random rand = new Random();
                            int value = rand.nextInt(countForEachPartition[entry]);
                            if(value<outputLimit){
                                reservior[entry][value]=h;
                            }
                        }
                    }
                    // System.out.println("a new");
                   /* try {
                        File temp=new File("/Users/MingdaLi/Desktop/ucla_3/asterixDB_lmd/asterixdb-1/hyracks-fullstack/hyracks/hyracks-dataflow-std/sample");
                        PrintStream ps = new PrintStream(new FileOutputStream(temp));

                        for (int k = 0; k < statePartitions; k++) {
                            for(int j=0;j<outputLimit;j++) {
                                ps.println(reservior[k][j]);
                                //System.out.println(reservior[k][j]);
                             //FrameUtils.appendToWriter(writer, partialAppender, fta, reservior[k][j]);
                                  //currentSize++;
                            }
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }*/
                    for (int i=0;i<statePartitions;++i){
                        reservior[i][outputLimit] =countForEachPartition[i];
                    }
                    Object reserviorObject=(Object) reservior;
                    ctx.setSharedObject(reserviorObject);
                    //partialAppender.write(writer, false);
                    //finished = true;

                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();

            }

            @Override
            public void close() throws HyracksDataException {
                Object reserviorObject=ctx.getSharedObject();
                int[][] reservior=new int[statePartitions][outputLimit+1];//store indexes for sample
                reservior= (int[][]) reserviorObject;//store indexes for sample

                int[] countForEachPartition=new int[statePartitions];
                // System.out.println("number in each pool");
                for (int i=0;i<statePartitions;++i){
                    countForEachPartition[i]=reservior[i][outputLimit];
                    //System.out.println(countForEachPartition[i]);
                }
                try {
                    File temp=new File("/Users/MingdaLi/Desktop/ucla_3/asterixDB_lmd/asterixdb-1/hyracks-fullstack/hyracks/hyracks-dataflow-std/sample");
                    PrintStream ps = new PrintStream(new FileOutputStream(temp));

                    for (int k = 0; k < statePartitions; k++) {
                        for(int j=0;j<outputLimit;j++) {
                            ps.println(reservior[k][j]);
                            //System.out.println(reservior[k][j]);
                            //FrameUtils.appendToWriter(writer, partialAppender, fta, reservior[k][j]);
                            //currentSize++;
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                writer.close();
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }
        };
    }

}
