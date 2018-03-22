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

package com.ibm.crail.namenode.rpc.darpc;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.ibm.crail.rpc.RpcErrors;
import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcNameNodeState;
import com.ibm.crail.rpc.RpcProtocol;
import com.ibm.crail.rpc.RpcRequestMessage;
import com.ibm.crail.rpc.RpcResponseMessage;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.DaRPCServerEvent;

public class DaRPCServiceDispatcherStats extends DaRPCServiceDispatcher {
	private static final Logger LOG = CrailUtils.getLogger();

	protected AtomicLong totalOps;
	protected AtomicLong createOps;
	protected AtomicLong lookupOps;
	protected AtomicLong setOps;
	protected AtomicLong removeOps;
	protected AtomicLong renameOps;
	protected AtomicLong getOps;
	protected AtomicLong locationOps;
	protected AtomicLong errorOps;

	public DaRPCServiceDispatcherStats(RpcNameNodeService service) {
		super(service);

		LOG.info("Enabled statistics.");

		this.totalOps = new AtomicLong(0);
		this.createOps = new AtomicLong(0);
		this.lookupOps = new AtomicLong(0);
		this.setOps = new AtomicLong(0);
		this.removeOps = new AtomicLong(0);
		this.renameOps = new AtomicLong(0);
		this.getOps = new AtomicLong(0);
		this.locationOps = new AtomicLong(0);
		this.errorOps = new AtomicLong(0);
	}

	public void processServerEvent(DaRPCServerEvent<DaRPCNameNodeRequest, DaRPCNameNodeResponse> event) {
		super.processServerEvent(event);
		DaRPCNameNodeRequest request = event.getReceiveMessage();
		switch(request.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				this.totalOps.incrementAndGet();
				this.createOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_GET_FILE:
				this.totalOps.incrementAndGet();
				this.lookupOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_SET_FILE:
				this.totalOps.incrementAndGet();
				this.setOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				this.totalOps.incrementAndGet();
				this.removeOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_RENAME_FILE:
				this.totalOps.incrementAndGet();
				this.renameOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_GET_BLOCK:
				this.totalOps.incrementAndGet();
				this.getOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_GET_LOCATION:
				this.totalOps.incrementAndGet();
				this.locationOps.incrementAndGet();
				break;
			case RpcProtocol.CMD_SET_BLOCK:
				break;
			case RpcProtocol.CMD_GET_DATANODE:
				break;
			case RpcProtocol.CMD_DUMP_NAMENODE:
				break;
			case RpcProtocol.CMD_PING_NAMENODE:
				break;
			default:
		}
	}
	public short stats(RpcRequestMessage.PingNameNodeReq request, RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState) throws Exception {
		short err = super.stats(request, response, errorState);

		LOG.info("totalOps " + totalOps.get());
		LOG.info("errorOps " + errorOps.get());
		LOG.info("createOps " + createOps.get());
		LOG.info("lookupOps " + lookupOps.get());
		LOG.info("setOps " + setOps.get());
		LOG.info("removeOps " + removeOps.get());
		LOG.info("renameOps " + renameOps.get());
		LOG.info("getOps " + getOps.get());
		LOG.info("locationOps " + locationOps.get());

		return err;
	}
}
