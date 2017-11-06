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

package com.ibm.crail.namenode;

import java.net.URI;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.rpc.RpcBinding;
import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcServer;
import com.ibm.crail.utils.CrailUtils;

public class NameNode {
	private static final Logger LOG = CrailUtils.getLogger();
	
	public static void main(String args[]) throws Exception {
		LOG.info("initalizing namenode ");		
		CrailConfiguration conf = new CrailConfiguration();
		CrailConstants.updateConstants(conf);
		
		URI uri = CrailUtils.getPrimaryNameNode();
		String address = uri.getHost();
		int port = uri.getPort();
		
		if (args != null) {
			Option addressOption = Option.builder("a").desc("ip address namenode is started on").hasArg().build();
			Option portOption = Option.builder("p").desc("port namenode is started on").hasArg().build();
			Options options = new Options();
			options.addOption(portOption);
			options.addOption(addressOption);
			CommandLineParser parser = new DefaultParser();
			
			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(addressOption.getOpt())) {
					address = line.getOptionValue(addressOption.getOpt());
				}					
				if (line.hasOption(portOption.getOpt())) {
					port = Integer.parseInt(line.getOptionValue(portOption.getOpt()));
				}				
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Namenode", options);
				System.exit(-1);
			}
		}		
		
		String namenode = "crail://" + address + ":" + port;
		long serviceId = CrailUtils.getServiceId(namenode);
		long serviceSize = CrailUtils.getServiceSize();
		if (!CrailUtils.verifyNamenode(namenode)){
			throw new Exception("Namenode address/port [" + namenode + "] has to be listed in crail.namenode.address " + CrailConstants.NAMENODE_ADDRESS);
		}
		
		CrailConstants.NAMENODE_ADDRESS = namenode + "?id=" + serviceId + "&size=" + serviceSize;
		CrailConstants.printConf();
		CrailConstants.verify();
		
		RpcNameNodeService service = RpcNameNodeService.createInstance(CrailConstants.NAMENODE_RPC_SERVICE);
		if (!CrailConstants.NAMENODE_LOG.isEmpty()){
			LogDispatcher logDispatcher = new LogDispatcher(service);
			service = logDispatcher;
		}
		RpcBinding rpcBinding = RpcBinding.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		RpcServer rpcServer = rpcBinding.launchServer(service);
		rpcServer.init(conf, null);
		rpcServer.printConf(LOG);
		rpcServer.run();
		System.exit(0);;
	}
}
