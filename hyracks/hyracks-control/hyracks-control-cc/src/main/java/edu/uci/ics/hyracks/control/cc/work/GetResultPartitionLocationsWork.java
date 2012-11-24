/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.work;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.IDatasetDirectoryService;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class GetResultPartitionLocationsWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final JobId jobId;
    private final NetworkAddress[] knownLocations;
    private final IResultCallback<NetworkAddress[]> callback;

    public GetResultPartitionLocationsWork(ClusterControllerService ccs, JobId jobId, NetworkAddress[] knownLocations,
            IResultCallback<NetworkAddress[]> callback) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.knownLocations = knownLocations;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        final IDatasetDirectoryService dds = ccs.getDatasetDirectoryService();
        ccs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    NetworkAddress[] partitionLocations = dds.getResultPartitionLocations(jobId, knownLocations);
                    callback.setValue(partitionLocations);
                } catch (HyracksDataException e) {
                    callback.setException(e);
                }
            }
        });
    }
}