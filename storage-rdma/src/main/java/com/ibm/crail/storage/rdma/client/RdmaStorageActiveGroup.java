/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.storage.rdma.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;

import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.rdma.MrCache;
import com.ibm.crail.storage.rdma.RdmaConstants;
import com.ibm.crail.storage.rdma.RdmaStorageGroup;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.rdma.*;

public class RdmaStorageActiveGroup extends RdmaActiveEndpointGroup<RdmaStorageActiveEndpoint> implements RdmaStorageGroup {
	private HashMap<InetSocketAddress, RdmaStorageLocalEndpoint> localCache;
	private MrCache mrCache;
	
	public RdmaStorageActiveGroup(int timeout, boolean polling, int maxWR, int maxSge, int cqSize, MrCache mrCache) throws IOException {
		super(timeout, polling, maxWR, maxSge, cqSize);
		try {
			this.mrCache = mrCache;
			this.localCache = new HashMap<InetSocketAddress, RdmaStorageLocalEndpoint>();
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		try {
			return createEndpoint(CrailUtils.datanodeInfo2SocketAddr(info));
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	//	@Override
	public StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws Exception {
		if (RdmaConstants.STORAGE_RDMA_LOCAL_MAP && CrailUtils.isLocalAddress(inetAddress.getAddress())){
			RdmaStorageLocalEndpoint localEndpoint = localCache.get(inetAddress.getAddress());
			if (localEndpoint == null){
				localEndpoint = new RdmaStorageLocalEndpoint(inetAddress);
				localCache.put(inetAddress, localEndpoint);
			}
			return localEndpoint;			
		}
		
		RdmaStorageActiveEndpoint endpoint = super.createEndpoint();
		URI uri = URI.create("rdma://" + inetAddress.getAddress().getHostAddress() + ":" + inetAddress.getPort());
		endpoint.connect(uri);
		return endpoint;
	}

	public int getType() {
		return 0;
	}

	@Override
	public String toString() {
		return "maxWR " + getMaxWR() + ", maxSge " + getMaxSge() + ", cqSize " + getCqSize();
	}

	public MrCache getMrCache() {
		return mrCache;
	}
}
