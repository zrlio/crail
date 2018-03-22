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

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.utils.CrailUtils;

public class DaRPCConstants {
	private static final Logger LOG = CrailUtils.getLogger();

	public static final String NAMENODE_DARPC_POLLING_KEY = "crail.namenode.darpc.polling";
	public static boolean NAMENODE_DARPC_POLLING = false;

	public static final String NAMENODE_DARPC_TYPE_KEY = "crail.namenode.darpc.type";
	public static String NAMENODE_DARPC_TYPE = "passive";

	public static final String NAMENODE_DARPC_AFFINITY_KEY = "crail.namenode.darpc.affinity";
	public static String NAMENODE_DARPC_AFFINITY = "1";

	public static final String NAMENODE_DARPC_MAXINLINE_KEY = "crail.namenode.darpc.maxinline";
	public static int NAMENODE_DARPC_MAXINLINE = 0;

	public static final String NAMENODE_DARPC_RECVQUEUE_KEY = "crail.namenode.darpc.recvQueue";
	public static int NAMENODE_DARPC_RECVQUEUE = 32;

	public static final String NAMENODE_DARPC_SENDQUEUE_KEY = "crail.namenode.darpc.sendQueue";
	public static int NAMENODE_DARPC_SENDQUEUE = 32;

	public static final String NAMENODE_DARPC_POLLSIZE_KEY = "crail.namenode.darpc.pollsize";
	public static int NAMENODE_DARPC_POLLSIZE = NAMENODE_DARPC_RECVQUEUE;

	public static final String NAMENODE_DARPC_CLUSTERSIZE_KEY = "crail.namenode.darpc.clustersize";
	public static int NAMENODE_DARPC_CLUSTERSIZE = 128;

	public static final String NAMENODE_DARPC_STATS_KEY = "crail.namenode.darpc.stats";
	public static boolean NAMENODE_DARPC_STATS = false;

	public static final String NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH_KEY = "crail.namenode.darpc.mempool.hugepagepath";
	public static String NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH = null;

	public static final String NAMENODE_DARPC_MEMPOOL_ALLOCSZ_KEY = "crail.namenode.darpc.mempool.allocationsize";
	public static int NAMENODE_DARPC_MEMPOOL_ALLOCSZ = 16 * 1024 * 1024; //16MB

	public static final String NAMENODE_DARPC_MEMPOOL_ALIGNMENT_KEY = "crail.namenode.darpc.mempool.alignment";
	public static int NAMENODE_DARPC_MEMPOOL_ALIGNMENT = 0;

	public static final String NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT_KEY = "crail.namenode.darpc.mempool.allocationlimit";
	public static int NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT = 16 * NAMENODE_DARPC_MEMPOOL_ALLOCSZ; //256MB;

	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(NAMENODE_DARPC_POLLING_KEY) != null) {
			NAMENODE_DARPC_POLLING = conf.getBoolean(NAMENODE_DARPC_POLLING_KEY, false);
		}
		if (conf.get(NAMENODE_DARPC_TYPE_KEY) != null) {
			NAMENODE_DARPC_TYPE = conf.get(NAMENODE_DARPC_TYPE_KEY);
		}
		if (conf.get(NAMENODE_DARPC_AFFINITY_KEY) != null) {
			NAMENODE_DARPC_AFFINITY = conf.get(NAMENODE_DARPC_AFFINITY_KEY);
		}
		if (conf.get(NAMENODE_DARPC_MAXINLINE_KEY) != null) {
			NAMENODE_DARPC_MAXINLINE = Integer.parseInt(conf.get(NAMENODE_DARPC_MAXINLINE_KEY));
		}
		if (conf.get(NAMENODE_DARPC_RECVQUEUE_KEY) != null) {
			NAMENODE_DARPC_RECVQUEUE = Integer.parseInt(conf.get(NAMENODE_DARPC_RECVQUEUE_KEY));
		}
		if (conf.get(NAMENODE_DARPC_SENDQUEUE_KEY) != null) {
			NAMENODE_DARPC_SENDQUEUE = Integer.parseInt(conf.get(NAMENODE_DARPC_SENDQUEUE_KEY));
		}
		if (conf.get(NAMENODE_DARPC_POLLSIZE_KEY) != null) {
			NAMENODE_DARPC_POLLSIZE = Integer.parseInt(conf.get(NAMENODE_DARPC_POLLSIZE_KEY));
		}
		if (conf.get(NAMENODE_DARPC_CLUSTERSIZE_KEY) != null) {
			NAMENODE_DARPC_CLUSTERSIZE = Integer.parseInt(conf.get(NAMENODE_DARPC_CLUSTERSIZE_KEY));
		}
		if (conf.get(NAMENODE_DARPC_STATS_KEY) != null) {
			NAMENODE_DARPC_STATS = Boolean.parseBoolean(conf.get(NAMENODE_DARPC_STATS_KEY));
		}
		if (conf.get(NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH_KEY) != null) {
			NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH = conf.get(NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH_KEY);
		}
		if (conf.get(NAMENODE_DARPC_MEMPOOL_ALLOCSZ_KEY) != null) {
			NAMENODE_DARPC_MEMPOOL_ALLOCSZ = Integer.parseInt(conf.get(NAMENODE_DARPC_MEMPOOL_ALLOCSZ_KEY));
		}
		if (conf.get(NAMENODE_DARPC_MEMPOOL_ALIGNMENT_KEY) != null) {
			NAMENODE_DARPC_MEMPOOL_ALIGNMENT = Integer.parseInt(conf.get(NAMENODE_DARPC_MEMPOOL_ALIGNMENT_KEY));
		}
		if (conf.get(NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT_KEY) != null) {
			NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT = Integer.parseInt(conf.get(NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT_KEY));
		}
	}

	public static void verify() throws IOException {
		if (!DaRPCConstants.NAMENODE_DARPC_TYPE.equalsIgnoreCase("passive") && !DaRPCConstants.NAMENODE_DARPC_TYPE.equalsIgnoreCase("active")){
			throw new IOException("crail.namenode.darpc.type must be either <active> or <passive>, found " + DaRPCConstants.NAMENODE_DARPC_TYPE);
		}
	}

	public static void printConf(Logger logger) {
		LOG.info(NAMENODE_DARPC_POLLING_KEY + " " + NAMENODE_DARPC_POLLING);
		LOG.info(NAMENODE_DARPC_TYPE_KEY + " " + NAMENODE_DARPC_TYPE);
		LOG.info(NAMENODE_DARPC_AFFINITY_KEY + " " + NAMENODE_DARPC_AFFINITY);
		LOG.info(NAMENODE_DARPC_MAXINLINE_KEY + " " + NAMENODE_DARPC_MAXINLINE);
		LOG.info(NAMENODE_DARPC_RECVQUEUE_KEY + " " + NAMENODE_DARPC_RECVQUEUE);
		LOG.info(NAMENODE_DARPC_SENDQUEUE_KEY + " " + NAMENODE_DARPC_SENDQUEUE);
		LOG.info(NAMENODE_DARPC_POLLSIZE_KEY + " " + NAMENODE_DARPC_POLLSIZE);
		LOG.info(NAMENODE_DARPC_CLUSTERSIZE_KEY + " " + NAMENODE_DARPC_CLUSTERSIZE);
		LOG.info(NAMENODE_DARPC_STATS_KEY + " " + NAMENODE_DARPC_STATS);
		LOG.info(NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH_KEY + " " + NAMENODE_DARPC_MEMPOOL_HUGEPAGEPATH);
		LOG.info(NAMENODE_DARPC_MEMPOOL_ALLOCSZ_KEY + " " + NAMENODE_DARPC_MEMPOOL_ALLOCSZ);
		LOG.info(NAMENODE_DARPC_MEMPOOL_ALIGNMENT_KEY + " " + NAMENODE_DARPC_MEMPOOL_ALIGNMENT);
		LOG.info(NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT_KEY + " " + NAMENODE_DARPC_MEMPOOL_ALLOC_LIMIT);
	}
}
