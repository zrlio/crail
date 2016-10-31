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

package com.ibm.crail.datanode.rdma.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.rdma.MrCache;
import com.ibm.crail.datanode.rdma.RdmaConstants;
import com.ibm.crail.datanode.rdma.RdmaDataNode;
import com.ibm.crail.datanode.rdma.RdmaDataNodeGroup;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.rdma.*;

public class RdmaDataNodePassiveGroup extends RdmaPassiveEndpointGroup<RdmaDataNodePassiveEndpoint> implements RdmaDataNodeGroup {
	private RdmaDataNodeLocalEndpoint localEndpoint;
	private MrCache mrCache;

	public RdmaDataNodePassiveGroup(int timeout, int maxWR, int maxSge, int cqSize, MrCache mrCache)
			throws IOException {
		super(timeout, maxWR, maxSge, cqSize);
		try {
			this.mrCache = mrCache;
			InetSocketAddress datanodeAddr = RdmaDataNode.getDataNodeAddress();
			if (datanodeAddr != null){
				this.localEndpoint = new RdmaDataNodeLocalEndpoint(datanodeAddr);
			} else {
				this.localEndpoint = null;
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	public DataNodeEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException {
		if (RdmaConstants.DATANODE_RDMA_LOCAL_MAP && CrailUtils.isLocalAddress(inetAddress.getAddress())){
			return this.localEndpoint;
		} 
		RdmaDataNodePassiveEndpoint endpoint = super.createEndpoint();
		try {
			endpoint.connect(inetAddress, 1000);
		} catch(Exception e){
			throw new IOException(e);
		}
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
