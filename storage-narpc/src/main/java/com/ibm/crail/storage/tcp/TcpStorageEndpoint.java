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

package com.ibm.crail.storage.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import com.ibm.crail.CrailBuffer;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.narpc.NaRPCEndpoint;
import com.ibm.narpc.NaRPCFuture;

public class TcpStorageEndpoint implements StorageEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();
	private NaRPCEndpoint<TcpStorageRequest, TcpStorageResponse> endpoint;
	
	public TcpStorageEndpoint(NaRPCEndpoint<TcpStorageRequest, TcpStorageResponse> endpoint) {
		this.endpoint = endpoint;
	}

	public void connect(InetSocketAddress address) throws IOException {
		endpoint.connect(address);
	}

	@Override
	public void close() throws IOException, InterruptedException {
		endpoint.close();
	}

	@Override
	public boolean isLocal() {
		return false;
	}

	@Override
	public StorageFuture read(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
//		LOG.info("TCP read, buffer " + buffer.remaining() + ", block " + block.getLkey() + "/" + block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		TcpStorageRequest.ReadRequest readReq = new TcpStorageRequest.ReadRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining());
		TcpStorageResponse.ReadResponse readResp = new TcpStorageResponse.ReadResponse(buffer.getByteBuffer());
		
		TcpStorageRequest req = new TcpStorageRequest(readReq);
		TcpStorageResponse resp = new TcpStorageResponse(readResp);
		
		NaRPCFuture<TcpStorageRequest, TcpStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new TcpStorageFuture(narpcFuture, readReq.length());
	}

	@Override
	public StorageFuture write(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
//		LOG.info("TCP write, buffer " + buffer.remaining() + ", block " +  block.getLkey() + "/" + block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		TcpStorageRequest.WriteRequest writeReq = new TcpStorageRequest.WriteRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining(), buffer.getByteBuffer());
		TcpStorageResponse.WriteResponse writeResp = new TcpStorageResponse.WriteResponse();
		
		TcpStorageRequest req = new TcpStorageRequest(writeReq);
		TcpStorageResponse resp = new TcpStorageResponse(writeResp);
		
		NaRPCFuture<TcpStorageRequest, TcpStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new TcpStorageFuture(narpcFuture, writeReq.length());
	}

}
