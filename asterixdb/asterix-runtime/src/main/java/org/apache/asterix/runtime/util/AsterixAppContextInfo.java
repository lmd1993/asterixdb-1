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
package org.apache.asterix.runtime.util;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.asterix.common.cluster.IGlobalRecoveryMaanger;
import org.apache.asterix.common.config.AsterixBuildProperties;
import org.apache.asterix.common.config.AsterixCompilerProperties;
import org.apache.asterix.common.config.AsterixExtensionProperties;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.AsterixPropertiesAccessor;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.dataflow.IAsterixApplicationContextInfo;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.transactions.IAsterixResourceIdManager;
import org.apache.hyracks.api.application.IApplicationConfig;
import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

/*
 * Acts as an holder class for IndexRegistryProvider, AsterixStorageManager
 * instances that are accessed from the NCs. In addition an instance of ICCApplicationContext
 * is stored for access by the CC.
 */
public class AsterixAppContextInfo implements IAsterixApplicationContextInfo, IAsterixPropertiesProvider {

    public static final AsterixAppContextInfo INSTANCE = new AsterixAppContextInfo();
    private ICCApplicationContext appCtx;
    private IGlobalRecoveryMaanger globalRecoveryMaanger;
    private ILibraryManager libraryManager;
    private IAsterixResourceIdManager resourceIdManager;
    private AsterixCompilerProperties compilerProperties;
    private AsterixExternalProperties externalProperties;
    private AsterixMetadataProperties metadataProperties;
    private AsterixStorageProperties storageProperties;
    private AsterixTransactionProperties txnProperties;
    private AsterixFeedProperties feedProperties;
    private AsterixBuildProperties buildProperties;
    private AsterixReplicationProperties replicationProperties;
    private AsterixExtensionProperties extensionProperties;
    private MessagingProperties messagingProperties;
    private IHyracksClientConnection hcc;
    private Object extensionManager;
    private volatile boolean initialized = false;

    private AsterixAppContextInfo() {
    }

    public static synchronized void initialize(ICCApplicationContext ccAppCtx, IHyracksClientConnection hcc,
            IGlobalRecoveryMaanger globalRecoveryMaanger, ILibraryManager libraryManager,
            IAsterixResourceIdManager resourceIdManager)
            throws AsterixException, IOException {
        if (INSTANCE.initialized) {
            throw new AsterixException(AsterixAppContextInfo.class.getSimpleName() + " has been initialized already");
        }
        INSTANCE.initialized = true;
        INSTANCE.appCtx = ccAppCtx;
        INSTANCE.hcc = hcc;
        INSTANCE.globalRecoveryMaanger = globalRecoveryMaanger;
        INSTANCE.libraryManager = libraryManager;
        INSTANCE.resourceIdManager = resourceIdManager;
        // Determine whether to use old-style asterix-configuration.xml or new-style configuration.
        // QQQ strip this out eventually
        AsterixPropertiesAccessor propertiesAccessor;
        IApplicationConfig cfg = ccAppCtx.getAppConfig();
        // QQQ this is NOT a good way to determine whether the config is valid
        if (cfg.getString("cc", "cluster.address") != null) {
            propertiesAccessor = new AsterixPropertiesAccessor(cfg);
        } else {
            propertiesAccessor = new AsterixPropertiesAccessor();
        }
        INSTANCE.compilerProperties = new AsterixCompilerProperties(propertiesAccessor);
        INSTANCE.externalProperties = new AsterixExternalProperties(propertiesAccessor);
        INSTANCE.metadataProperties = new AsterixMetadataProperties(propertiesAccessor);
        INSTANCE.storageProperties = new AsterixStorageProperties(propertiesAccessor);
        INSTANCE.txnProperties = new AsterixTransactionProperties(propertiesAccessor);
        INSTANCE.feedProperties = new AsterixFeedProperties(propertiesAccessor);
        INSTANCE.extensionProperties = new AsterixExtensionProperties(propertiesAccessor);
        INSTANCE.replicationProperties = new AsterixReplicationProperties(propertiesAccessor,
                AsterixClusterProperties.INSTANCE.getCluster());
        INSTANCE.hcc = hcc;
        INSTANCE.buildProperties = new AsterixBuildProperties(propertiesAccessor);
        INSTANCE.messagingProperties = new MessagingProperties(propertiesAccessor);
        Logger.getLogger("org.apache").setLevel(INSTANCE.externalProperties.getLogLevel());
    }

    public boolean initialized() {
        return initialized;
    }

    @Override
    public ICCApplicationContext getCCApplicationContext() {
        return appCtx;
    }

    @Override
    public AsterixStorageProperties getStorageProperties() {
        return storageProperties;
    }

    @Override
    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public AsterixCompilerProperties getCompilerProperties() {
        return compilerProperties;
    }

    @Override
    public AsterixMetadataProperties getMetadataProperties() {
        return metadataProperties;
    }

    @Override
    public AsterixExternalProperties getExternalProperties() {
        return externalProperties;
    }

    @Override
    public AsterixFeedProperties getFeedProperties() {
        return feedProperties;
    }

    @Override
    public AsterixBuildProperties getBuildProperties() {
        return buildProperties;
    }

    public IHyracksClientConnection getHcc() {
        return hcc;
    }

    @Override
    public IIndexLifecycleManagerProvider getIndexLifecycleManagerProvider() {
        return AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
    }

    @Override
    public AsterixReplicationProperties getReplicationProperties() {
        return replicationProperties;
    }

    @Override
    public IGlobalRecoveryMaanger getGlobalRecoveryManager() {
        return globalRecoveryMaanger;
    }

    @Override
    public ILibraryManager getLibraryManager() {
        return libraryManager;
    }

    public Object getExtensionManager() {
        return extensionManager;
    }

    public void setExtensionManager(Object extensionManager) {
        this.extensionManager = extensionManager;
    }

    public AsterixExtensionProperties getExtensionProperties() {
        return extensionProperties;
    }

    @Override
    public MessagingProperties getMessagingProperties() {
        return messagingProperties;
    }

    public IAsterixResourceIdManager getResourceIdManager() {
        return resourceIdManager;
    }
}
