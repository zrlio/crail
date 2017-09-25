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

import org.slf4j.Logger;

import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcProtocol;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.DaRPCServerEvent;

class CounterThread extends Thread {
	public void run() {
		long oldsum = 0;
		long sum = 0;
		long oldtime = System.currentTimeMillis();
		long currTime;
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (int i = 0; i < DaRPCServiceDispatcherStats.counts.length; i++) {
				sum += DaRPCServiceDispatcherStats.counts[i];
			}
			currTime = System.currentTimeMillis();
			System.out.println("Statistics: "
					+ String.format("%.2f", (double)(sum - oldsum)/((double)(currTime - oldtime))*(double)1000.0) + " IOPS"
					+ ", diff: " + (sum - oldsum)
					+ ", time diff(ms): " + (currTime - oldtime));
			oldtime = currTime;
			oldsum = sum;
			sum = 0;
		}
	}
}



public class DaRPCServiceDispatcherStats extends DaRPCServiceDispatcher {
	private static final Logger LOG = CrailUtils.getLogger();
	private static final int maxNrThreads = 200;

	public static volatile long counts[];
	public static volatile long oldcounts[];
	public static volatile long times[];
	public static volatile Thread counterThread = null;


	public DaRPCServiceDispatcherStats(RpcNameNodeService service) {
		super(service);
		
		counts = new long[maxNrThreads * 8]; //use every 8th element to avoid false cache sharing
		oldcounts = new long[maxNrThreads * 8];
		times = new long[maxNrThreads * 8];
		long currTime = System.currentTimeMillis();
		for (int i = 0; i < times.length; i++) {
			times[i] = currTime;
		}
		if (counterThread == null) {
			counterThread = new CounterThread();
			counterThread.start();
		}

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

		//use every 8th element to avoid false cache sharing
		int myID = (int)Thread.currentThread().getId() * 8;
		counts[myID]++;
	}
}
