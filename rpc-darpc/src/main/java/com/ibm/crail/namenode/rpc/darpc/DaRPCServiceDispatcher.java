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
import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.rpc.RpcErrors;
import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcNameNodeState;
import com.ibm.crail.rpc.RpcProtocol;
import com.ibm.crail.rpc.RpcRequestMessage;
import com.ibm.crail.rpc.RpcResponseMessage;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerEvent;
import com.ibm.darpc.DaRPCService;

public class DaRPCServiceDispatcher extends DaRPCNameNodeProtocol implements DaRPCService<DaRPCNameNodeRequest, DaRPCNameNodeResponse> {
	private static final Logger LOG = CrailUtils.getLogger();

	private RpcNameNodeService service;


	public DaRPCServiceDispatcher(RpcNameNodeService service){
		this.service = service;
	}

	public void processServerEvent(DaRPCServerEvent<DaRPCNameNodeRequest, DaRPCNameNodeResponse> event) {
		DaRPCNameNodeRequest request = event.getReceiveMessage();
		DaRPCNameNodeResponse response = event.getSendMessage();
		short error = RpcErrors.ERR_OK;
		try {
			response.setType(RpcProtocol.responseTypes[request.getCmd()]);
			response.setError((short) 0);
			switch(request.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				error = service.createFile(request.createFile(), response.createFile(), response);
				break;
			case RpcProtocol.CMD_GET_FILE:
				error = service.getFile(request.getFile(), response.getFile(), response);
				break;
			case RpcProtocol.CMD_SET_FILE:
				error = service.setFile(request.setFile(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(request.removeFile(), response.delFile(), response);
				break;
			case RpcProtocol.CMD_RENAME_FILE:
				error = service.renameFile(request.renameFile(), response.getRename(), response);
				break;
			case RpcProtocol.CMD_GET_BLOCK:
				error = service.getBlock(request.getBlock(), response.getBlock(), response);
				break;
			case RpcProtocol.CMD_GET_LOCATION:
				error = service.getLocation(request.getLocation(), response.getLocation(), response);
				break;
			case RpcProtocol.CMD_SET_BLOCK:
				error = service.setBlock(request.setBlock(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_GET_DATANODE:
				error = service.getDataNode(request.getDataNode(), response.getDataNode(), response);
				break;
			case RpcProtocol.CMD_DUMP_NAMENODE:
				error = service.dump(request.dumpNameNode(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_PING_NAMENODE:
				error = this.stats(request.pingNameNode(), response.pingNameNode(), response);
				error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
				break;
			default:
				error = RpcErrors.ERR_INVALID_RPC_CMD;
				LOG.info("Rpc command not valid, opcode " + request.getCmd());
			}
		} catch(Exception e){
			error = RpcErrors.ERR_UNKNOWN;
			//this.errorOps.incrementAndGet();
			LOG.info(RpcErrors.messages[RpcErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}

		try {
			response.setError(error);
			event.triggerResponse();
		} catch(Exception e){
			LOG.info("ERROR: RPC failed, messagesSend ");
			e.printStackTrace();
		}
	}

	public short stats(RpcRequestMessage.PingNameNodeReq request, RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState) throws Exception {
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_PING_NAMENODE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}
		return RpcErrors.ERR_OK;
	}

	@Override
	public void open(DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> endpoint) {
		try {
			LOG.info("RPC connection, qpnum " + endpoint.getQp().getQp_num());
		} catch(IOException e) {
			LOG.info("RPC connection, cannot get qpnum, because QP is not open.\n");
		}
	}

	@Override
	public void close(DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> endpoint) {
		try {
			LOG.info("disconnecting RPC connection, qpnum " + endpoint.getQp().getQp_num());
			endpoint.close();
		} catch(Exception e){
		}
	}
}
