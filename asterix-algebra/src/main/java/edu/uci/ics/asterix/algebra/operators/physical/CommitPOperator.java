/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.algebra.operators.physical;

import java.util.List;

import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class CommitPOperator extends AbstractPhysicalOperator {

    private final List<LogicalVariable> primaryKeyLogicalVars;
    private final JobId jobId;
    private final int datasetId;

    public CommitPOperator(JobId jobId, int datasetId, List<LogicalVariable> primaryKeyLogicalVars) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyLogicalVars = primaryKeyLogicalVars;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.EXTENSION_OPERATOR;
    }

    @Override
    public String toString() {
        return "COMMIT";
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {

        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema,
                context);
        int[] primaryKeyFields = JobGenHelper.variablesToFieldIndexes(primaryKeyLogicalVars, inputSchemas[0]);
        IVariableTypeEnvironment varTypeEnv = context.getTypeEnvironment(op.getInputs().get(0).getValue());
        IBinaryHashFunctionFactory[] binaryHashFunctionFactories = JobGenHelper.variablesToBinaryHashFunctionFactories(
                primaryKeyLogicalVars, varTypeEnv, context);

        CommitRuntimeFactory runtime = new CommitRuntimeFactory(jobId, datasetId, primaryKeyFields,
                binaryHashFunctionFactories);
        builder.contributeMicroOperator(op, runtime, recDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }
}