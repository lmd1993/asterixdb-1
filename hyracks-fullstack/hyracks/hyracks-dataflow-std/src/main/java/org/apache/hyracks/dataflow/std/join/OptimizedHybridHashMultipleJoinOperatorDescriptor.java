/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.join;

import javafx.scene.effect.Bloom;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.base.*;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;
import org.apache.hyracks.dataflow.std.util.BloomFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mingda Li
 *         This is extended to deal wtih star schema multiple join.
 *         The input is the Bloom Filter of A, C.
 *
 */

public class OptimizedHybridHashMultipleJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private static final double NLJ_SWITCH_THRESHOLD = 0.8;

    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";

    private final int frameLimit;
    private final int inputsize0;
    private final double fudgeFactor;
    private final int[] probeKeys;
    private final int[] buildKeys;
    private final int[] buildKeys2;
    private final int[] probeKeys2;
    BloomFilter<String> B2Abf;
    BloomFilter<String> B2Cbf;
    double falsePositiveProbability = 0.1;
    int expectedNumberOfElements = 100000;//should calculate from the size
    private final IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories;
    private final IBinaryComparatorFactory[] comparatorFactories; //For in-mem HJ
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryProbe2Build; //For NLJ in probe
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryBuild2Probe; //For NLJ in probe
    private final IPredicateEvaluatorFactory predEvaluatorFactory;
    private final String Bfile;
    private final boolean isLeftOuter;
    private final IMissingWriterFactory[] nonMatchWriterFactories;

    //Flags added for test purpose
    private static boolean skipInMemoryHJ = false;
    private static boolean forceNLJ = false;
    private static boolean forceRR = false;

    private static final Logger LOGGER = Logger.getLogger(OptimizedHybridHashMultipleJoinOperatorDescriptor.class.getName());

    public OptimizedHybridHashMultipleJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int inputsize0,
                                                             double factor, int[] keys0, int[] keys1,int[]keys2, int[]keys3,String Bfile, IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories,
                                                             IBinaryComparatorFactory[] comparatorFactories,

                                                             RecordDescriptor recordDescriptor,
                                                             ITuplePairComparatorFactory tupPaircomparatorFactory01,
                                                             ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory,
                                                             boolean isLeftOuter, IMissingWriterFactory[] nonMatchWriterFactories) throws HyracksDataException {

        super(spec, 2, 1);
        this.frameLimit = frameLimit;
        this.inputsize0 = inputsize0;
        this.fudgeFactor = factor;
        this.probeKeys = keys0;// For table A
        this.buildKeys = keys1;// For table B's attribute to join with A
        this.buildKeys2=keys2;// For table B's attribute to join with C
        this.probeKeys2=keys3;// For table C
        this.expectedNumberOfElements = 100000;//should calculate from the size
        this.B2Abf=new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
        this.B2Cbf=new BloomFilter<String>(falsePositiveProbability,expectedNumberOfElements);
        this.Bfile=Bfile;//B file to get the probable item numbers; file size/ one item's size
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
        this.comparatorFactories = comparatorFactories;
        this.tuplePairComparatorFactoryProbe2Build = tupPaircomparatorFactory01;
        this.tuplePairComparatorFactoryBuild2Probe = tupPaircomparatorFactory10;
        recordDescriptors[0] = recordDescriptor;
        this.predEvaluatorFactory = predEvaluatorFactory;
        this.isLeftOuter = isLeftOuter;
        this.nonMatchWriterFactories = nonMatchWriterFactories;
    }

    public OptimizedHybridHashMultipleJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int inputsize0,
                                                             double factor, int[] keys0, int[] keys1,int[] keys2, int[] keys3, String Bfile,IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories,
                                                             IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
                                                             ITuplePairComparatorFactory tupPaircomparatorFactory01,
                                                             ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory)
            throws HyracksDataException {
        this(spec, frameLimit, inputsize0, factor, keys0, keys1,keys2,keys3, Bfile, hashFunctionGeneratorFactories, comparatorFactories,
                recordDescriptor, tupPaircomparatorFactory01, tupPaircomparatorFactory10, predEvaluatorFactory, false,
                null);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId buildAid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId probeAid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        PartitionAndBuildActivityNode phase1 = new PartitionAndBuildActivityNode(buildAid, probeAid);
        ProbeAndJoinActivityNode phase2 = new ProbeAndJoinActivityNode(probeAid, buildAid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);
        builder.addSourceEdge(2,phase2,0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);

    }

    //memorySize is the memory for join (we have already excluded the 2 buffers for in/out)
    private int getNumberOfPartitions(int memorySize, int buildSize, double factor, int nPartitions)
            throws HyracksDataException {
        int numberOfPartitions = 0;
        if (memorySize <= 1) {
            throw new HyracksDataException("not enough memory is available for Hybrid Hash Join");
        }
        if (memorySize > buildSize) {
            return 1; //We will switch to in-Mem HJ eventually
        }
        numberOfPartitions = (int) (Math.ceil((buildSize * factor / nPartitions - memorySize) / (memorySize - 1)));
        if (numberOfPartitions <= 0) {
            numberOfPartitions = 1; //becomes in-memory hash join
        }
        if (numberOfPartitions > memorySize) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            return (numberOfPartitions < memorySize ? numberOfPartitions : memorySize);
        }
        return numberOfPartitions;
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {

        private int memForJoin;
        private int numOfPartitions;
        private OptimizedHybridHashJoin hybridHJ;
        private OptimizedHybridHashJoin hybridHJ2;
        BloomFilter <String> BF1;
        BloomFilter <String> BF2;

        public BuildAndPartitionTaskState() {
        }
        public void setBF(BloomFilter<String>bf1, BloomFilter<String> bf2){
            this.BF1=bf1;
            this.BF2=bf2;
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }


        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }

    /*
     * Build phase of Hybrid Hash Join:
     * Creating an instance of Hybrid Hash Join, using Shapiro's formula
     * to get the optimal number of partitions, build relation is read and
     * partitioned, and hybrid hash join instance gets ready for the probing.
     * (See OptimizedHybridHashJoin for the details on different steps)
     */
    private class PartitionAndBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId probeAid;

        public PartitionAndBuildActivityNode(ActivityId id, ActivityId probeAid) {
            super(id);
            this.probeAid = probeAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {

            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(probeAid, 0);

            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            final IPredicateEvaluator predEvaluator = (predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator());

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(
                        ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));

                ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                        hashFunctionGeneratorFactories).createPartitioner(0);
                ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                        hashFunctionGeneratorFactories).createPartitioner(0);

                ITuplePartitionComputer probeHpc2 = new FieldHashPartitionComputerFamily(probeKeys2,
                        hashFunctionGeneratorFactories).createPartitioner(0);
                ITuplePartitionComputer buildHpc2 = new FieldHashPartitionComputerFamily(buildKeys2,
                        hashFunctionGeneratorFactories).createPartitioner(0);
                boolean isFailed = false;

                @Override
                public void open() throws HyracksDataException {
                    if (frameLimit <= 2) { //Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for Hybrid Hash Join");
                    }
                    state.memForJoin = frameLimit - 2;
                    state.numOfPartitions = getNumberOfPartitions(state.memForJoin, inputsize0, fudgeFactor,
                            nPartitions);
                    state.hybridHJ = new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                            PROBE_REL, BUILD_REL, probeKeys, buildKeys, comparators, probeRd, buildRd, probeHpc,
                            buildHpc, predEvaluator, isLeftOuter, nonMatchWriterFactories);
                    state.hybridHJ2=new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                            PROBE_REL, BUILD_REL, probeKeys2, buildKeys2, comparators, probeRd, buildRd, probeHpc2,
                            buildHpc2, predEvaluator, isLeftOuter, nonMatchWriterFactories);

                    state.hybridHJ.initBuild();
                    state.hybridHJ2.initBuild();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin is starting the build phase with " + state.numOfPartitions
                                + " partitions using " + state.memForJoin + " frames for memory.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    BloomFilter<String>[] BFforB=new BloomFilter[2]; //BloomFilter[0] for A, [1] for C
                    if(ctx.getSharedObject()!=null){
                        Object BFObject=ctx.getSharedObject();
                        BFforB = (BloomFilter<String>[]) BFObject;
                    }
                    state.hybridHJ.buildWithBF(buffer, BFforB[0],buildKeys);//add key to each bloom filter
                    state.hybridHJ2.buildWithBF(buffer,BFforB[1],buildKeys2);
                    Object BFObject= (Object) BFforB;
                    ctx.setSharedObject(BFObject);
                }

                @Override
                public void close() throws HyracksDataException {
                    Object BFObject=ctx.getSharedObject();
                    BloomFilter<String>[] BFforB = (BloomFilter<String>[]) BFObject;
                    state.setBF(BFforB[0], BFforB[1]);
                    state.hybridHJ.closeBuild();
                    state.hybridHJ2.closeBuild();
                    if (isFailed) {
                        state.hybridHJ.clearBuildTempFiles();
                        state.hybridHJ2.clearBuildTempFiles();
                    } else {
                        ctx.setStateObject(state);
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("OptimizedHybridHashJoin closed its build phase");
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    isFailed = true;
                }

            };
            return op;
        }
    }

    /*
     * Probe phase of Hybrid Hash Join:
     * Reading the probe side and partitioning it, resident tuples get
     * joined with the build side residents (through formerly created HybridHashJoin in the build phase)
     * and spilled partitions get written to run files. During the close() call, pairs of spilled partition
     * (build side spilled partition and its corresponding probe side spilled partition) join, by applying
     * Hybrid Hash Join recursively on them.
     */
    private class ProbeAndJoinActivityNode extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        private final ActivityId buildAid;

        public ProbeAndJoinActivityNode(ActivityId id, ActivityId buildAid) {
            super(id);
            this.buildAid = buildAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {

            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(buildAid, 0);
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            final ITuplePairComparator nljComparatorProbe2Build = tuplePairComparatorFactoryProbe2Build
                    .createTuplePairComparator(ctx);
            final ITuplePairComparator nljComparatorBuild2Probe = tuplePairComparatorFactoryBuild2Probe
                    .createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator = predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator();

            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            final IMissingWriter[] nonMatchWriter = isLeftOuter ? new IMissingWriter[nonMatchWriterFactories.length]
                    : null;
            final ArrayTupleBuilder nullTupleBuild = isLeftOuter ? new ArrayTupleBuilder(buildRd.getFieldCount())
                    : null;
            if (isLeftOuter) {
                DataOutput out = nullTupleBuild.getDataOutput();
                for (int i = 0; i < nonMatchWriterFactories.length; i++) {
                    nonMatchWriter[i] = nonMatchWriterFactories[i].createMissingWriter();
                    nonMatchWriter[i].writeMissing(out);
                    nullTupleBuild.addFieldEndOffset();
                }
            }

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;
                private IFrame rPartbuff = new VSizeFrame(ctx);

                private FrameTupleAppender nullResultAppender = null;
                private FrameTupleAccessor probeTupleAccessor;

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), BUILD_AND_PARTITION_ACTIVITY_ID), partition));

                    writer.open();
                    state.hybridHJ.initProbe();

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin is starting the probe phase.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.probe(buffer, writer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    try {
                        state.hybridHJ.closeProbe(writer);

                        BitSet partitionStatus = state.hybridHJ.getPartitionStatus();

                        rPartbuff.reset();
                        for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid = partitionStatus
                                .nextSetBit(pid + 1)) {

                            RunFileReader bReader = state.hybridHJ.getBuildRFReader(pid);
                            RunFileReader pReader = state.hybridHJ.getProbeRFReader(pid);

                            if (bReader == null || pReader == null) {
                                if (isLeftOuter && pReader != null) {
                                    appendNullToProbeTuples(pReader);
                                }
                                continue;
                            }
                            int bSize = state.hybridHJ.getBuildPartitionSizeInTup(pid);
                            int pSize = state.hybridHJ.getProbePartitionSizeInTup(pid);
                            joinPartitionPair(bReader, pReader, bSize, pSize, 1);
                        }
                    } finally {
                        writer.close();
                    }
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin closed its probe phase");
                    }
                }

                //The buildSideReader should be always the original buildSideReader, so should the probeSideReader
                private void joinPartitionPair(RunFileReader buildSideReader, RunFileReader probeSideReader,
                        int buildSizeInTuple, int probeSizeInTuple, int level) throws HyracksDataException {
                    ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);
                    ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);

                    long buildPartSize = buildSideReader.getFileSize() / ctx.getInitialFrameSize();
                    long probePartSize = probeSideReader.getFileSize() / ctx.getInitialFrameSize();
                    int beforeMax = Math.max(buildSizeInTuple, probeSizeInTuple);

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("\n>>>Joining Partition Pairs (thread_id " + Thread.currentThread().getId()
                                + ") (pid " + ") - (level " + level + ")" + " - BuildSize:\t" + buildPartSize
                                + "\tProbeSize:\t" + probePartSize + " - MemForJoin " + (state.memForJoin)
                                + "  - LeftOuter is " + isLeftOuter);
                    }

                    //Apply in-Mem HJ if possible
                    if (!skipInMemoryHJ && ((buildPartSize < state.memForJoin)
                            || (probePartSize < state.memForJoin && !isLeftOuter))) {
                        int tabSize = -1;
                        if (!forceRR && (isLeftOuter || (buildPartSize < probePartSize))) {
                            //Case 1.1 - InMemHJ (wout Role-Reversal)
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("\t>>>Case 1.1 (IsLeftOuter || buildSize<probe) AND ApplyInMemHJ - [Level "
                                        + level + "]");
                            }
                            tabSize = buildSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Build Side is smaller
                            applyInMemHashJoin(buildKeys, probeKeys, tabSize, buildRd, probeRd, buildHpc, probeHpc,
                                    buildSideReader, probeSideReader); // checked-confirmed
                        } else { //Case 1.2 - InMemHJ with Role Reversal
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine(
                                        "\t>>>Case 1.2. (NoIsLeftOuter || probe<build) AND ApplyInMemHJ WITH RoleReversal - [Level "
                                                + level + "]");
                            }
                            tabSize = probeSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Probe Side is smaller
                            applyInMemHashJoin(probeKeys, buildKeys, tabSize, probeRd, buildRd, probeHpc, buildHpc,
                                    probeSideReader, buildSideReader); // checked-confirmed
                        }
                    }
                    //Apply (Recursive) HHJ
                    else {
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("\t>>>Case 2. ApplyRecursiveHHJ - [Level " + level + "]");
                        }
                        if (!forceRR && (isLeftOuter || buildPartSize < probePartSize)) {
                            //Case 2.1 - Recursive HHJ (wout Role-Reversal)
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("\t\t>>>Case 2.1 - RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                        + level + "]");
                            }
                            applyHybridHashJoin((int) buildPartSize, PROBE_REL, BUILD_REL, probeKeys, buildKeys,
                                    probeRd, buildRd, probeHpc, buildHpc, probeSideReader, buildSideReader, level,
                                    beforeMax);

                        } else { //Case 2.2 - Recursive HHJ (with Role-Reversal)
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine(
                                        "\t\t>>>Case 2.2. - RecursiveHHJ WITH RoleReversal - [Level " + level + "]");
                            }

                            applyHybridHashJoin((int) probePartSize, BUILD_REL, PROBE_REL, buildKeys, probeKeys,
                                    buildRd, probeRd, buildHpc, probeHpc, buildSideReader, probeSideReader, level,
                                    beforeMax);

                        }
                    }
                }

                private void applyHybridHashJoin(int tableSize, final String PROBE_REL, final String BUILD_REL,
                        final int[] probeKeys, final int[] buildKeys, final RecordDescriptor probeRd,
                        final RecordDescriptor buildRd, final ITuplePartitionComputer probeHpc,
                        final ITuplePartitionComputer buildHpc, RunFileReader probeSideReader,
                        RunFileReader buildSideReader, final int level, final long beforeMax)
                        throws HyracksDataException {

                    boolean isReversed = probeKeys == OptimizedHybridHashMultipleJoinOperatorDescriptor.this.buildKeys
                            && buildKeys == OptimizedHybridHashMultipleJoinOperatorDescriptor.this.probeKeys;

                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";

                    OptimizedHybridHashJoin rHHj;
                    int n = getNumberOfPartitions(state.memForJoin, tableSize, fudgeFactor, nPartitions);
                    rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, PROBE_REL, BUILD_REL, probeKeys,
                            buildKeys, comparators, probeRd, buildRd, probeHpc, buildHpc, predEvaluator, isLeftOuter,
                            nonMatchWriterFactories); //checked-confirmed

                    rHHj.setIsReversed(isReversed);
                    buildSideReader.open();
                    rHHj.initBuild();
                    rPartbuff.reset();
                    while (buildSideReader.nextFrame(rPartbuff)) {
                        rHHj.build(rPartbuff.getBuffer());
                    }
                    rHHj.closeBuild();
                    buildSideReader.close();

                    probeSideReader.open();
                    rHHj.initProbe();
                    rPartbuff.reset();
                    while (probeSideReader.nextFrame(rPartbuff)) {
                        rHHj.probe(rPartbuff.getBuffer(), writer);
                    }
                    rHHj.closeProbe(writer);
                    probeSideReader.close();

                    int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                    int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                    int afterMax = Math.max(maxAfterBuildSize, maxAfterProbeSize);

                    BitSet rPStatus = rHHj.getPartitionStatus();
                    if (!forceNLJ && (afterMax < (NLJ_SWITCH_THRESHOLD * beforeMax))) { //Case 2.1.1 - Keep applying HHJ
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine(
                                    "\t\t>>>Case 2.1.1 - KEEP APPLYING RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                            + level + "]");
                        }
                        for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                            RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                            RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
                            int rbSizeInTuple = rHHj.getBuildPartitionSizeInTup(rPid);
                            int rpSizeInTuple = rHHj.getProbePartitionSizeInTup(rPid);

                            if (rbrfw == null || rprfw == null) {
                                if (isLeftOuter && rprfw != null) { // if outer join, we don't reverse
                                    appendNullToProbeTuples(rprfw);
                                }
                                continue;
                            }

                            if (isReversed) {
                                joinPartitionPair(rprfw, rbrfw, rpSizeInTuple, rbSizeInTuple, level + 1);
                            } else {
                                joinPartitionPair(rbrfw, rprfw, rbSizeInTuple, rpSizeInTuple, level + 1);
                            }
                        }

                    } else { //Case 2.1.2 - Switch to NLJ
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine(
                                    "\t\t>>>Case 2.1.2 - SWITCHED to NLJ RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                            + level + "]");
                        }
                        for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                            RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                            RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                            if (rbrfw == null || rprfw == null) {
                                if (isLeftOuter && rprfw != null) { // if outer join, we don't reverse
                                    appendNullToProbeTuples(rprfw);
                                }
                                continue;
                            }

                            int buildSideInTups = rHHj.getBuildPartitionSizeInTup(rPid);
                            int probeSideInTups = rHHj.getProbePartitionSizeInTup(rPid);
                            // NLJ order is outer + inner, the order is reversed from the other joins
                            if (isLeftOuter || probeSideInTups < buildSideInTups) {
                                applyNestedLoopJoin(probeRd, buildRd, frameLimit, rprfw, rbrfw); //checked-modified
                            } else {
                                applyNestedLoopJoin(buildRd, probeRd, frameLimit, rbrfw, rprfw); //checked-modified
                            }
                        }
                    }
                }

                private void appendNullToProbeTuples(RunFileReader probReader) throws HyracksDataException {
                    if (nullResultAppender == null) {
                        nullResultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    }
                    if (probeTupleAccessor == null) {
                        probeTupleAccessor = new FrameTupleAccessor(probeRd);
                    }
                    probReader.open();
                    while (probReader.nextFrame(rPartbuff)) {
                        probeTupleAccessor.reset(rPartbuff.getBuffer());
                        for (int tid = 0; tid < probeTupleAccessor.getTupleCount(); tid++) {
                            FrameUtils.appendConcatToWriter(writer, nullResultAppender, probeTupleAccessor, tid,
                                    nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0,
                                    nullTupleBuild.getSize());
                        }
                    }
                    probReader.close();
                    nullResultAppender.write(writer, true);
                }

                private void applyInMemHashJoin(int[] bKeys, int[] pKeys, int tabSize, RecordDescriptor buildRDesc,
                        RecordDescriptor probeRDesc, ITuplePartitionComputer hpcRepBuild,
                        ITuplePartitionComputer hpcRepProbe, RunFileReader bReader, RunFileReader pReader)
                        throws HyracksDataException {
                    boolean isReversed = pKeys == OptimizedHybridHashMultipleJoinOperatorDescriptor.this.buildKeys
                            && bKeys == OptimizedHybridHashMultipleJoinOperatorDescriptor.this.probeKeys;

                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";

                    ISerializableTable table = new SerializableHashTable(tabSize, ctx);
                    InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tabSize, new FrameTupleAccessor(probeRDesc),
                            hpcRepProbe, new FrameTupleAccessor(buildRDesc), hpcRepBuild,
                            new FrameTuplePairComparator(pKeys, bKeys, comparators), isLeftOuter, nonMatchWriter, table,
                            predEvaluator, isReversed);

                    bReader.open();
                    rPartbuff.reset();
                    while (bReader.nextFrame(rPartbuff)) {
                        //We need to allocate a copyBuffer, because this buffer gets added to the buffers list in the InMemoryHashJoin
                        ByteBuffer copyBuffer = ctx.allocateFrame(rPartbuff.getFrameSize());
                        FrameUtils.copyAndFlip(rPartbuff.getBuffer(), copyBuffer);
                        joiner.build(copyBuffer);
                        rPartbuff.reset();
                    }
                    bReader.close();
                    rPartbuff.reset();
                    //probe
                    pReader.open();
                    while (pReader.nextFrame(rPartbuff)) {
                        joiner.join(rPartbuff.getBuffer(), writer);
                        rPartbuff.reset();
                    }
                    pReader.close();
                    joiner.closeJoin(writer);
                }

                private void applyNestedLoopJoin(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize,
                        RunFileReader outerReader, RunFileReader innerReader) throws HyracksDataException {
                    // The nested loop join result is outer + inner. All the other operator is probe + build. Hence the reverse relation is different
                    boolean isReversed = outerRd == buildRd && innerRd == probeRd;
                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";
                    ITuplePairComparator nljComptorOuterInner = isReversed ? nljComparatorBuild2Probe
                            : nljComparatorProbe2Build;
                    NestedLoopJoin nlj = new NestedLoopJoin(ctx, new FrameTupleAccessor(outerRd),
                            new FrameTupleAccessor(innerRd), nljComptorOuterInner, memorySize, predEvaluator,
                            isLeftOuter, nonMatchWriter);
                    nlj.setIsReversed(isReversed);

                    IFrame cacheBuff = new VSizeFrame(ctx);
                    innerReader.open();
                    while (innerReader.nextFrame(cacheBuff)) {
                        nlj.cache(cacheBuff.getBuffer());
                        cacheBuff.reset();
                    }
                    nlj.closeCache();

                    IFrame joinBuff = new VSizeFrame(ctx);
                    outerReader.open();

                    while (outerReader.nextFrame(joinBuff)) {
                        nlj.join(joinBuff.getBuffer(), writer);
                        joinBuff.reset();
                    }

                    nlj.closeJoin(writer);
                    outerReader.close();
                    innerReader.close();
                }
            };
            return op;
        }
    }

    public void setSkipInMemHJ(boolean b) {
        skipInMemoryHJ = b;
    }

    public void setForceNLJ(boolean b) {
        forceNLJ = b;
    }

    public void setForceRR(boolean b) {
        forceRR = (!isLeftOuter && b);
    }
}
