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

package com.ibm.crail.metadata;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class BlockInfo {
	public static int CSIZE = DataNodeInfo.CSIZE + 24;
	
	protected DataNodeInfo dnInfo;
	protected long lba;
	protected long addr;
	protected int length;
	protected int lkey;	
	
	public BlockInfo(){
		this.dnInfo = new DataNodeInfo();
		this.lba = 0;
		this.addr = 0;
		this.length = 0;
		this.lkey = 0;
	}
	
	public BlockInfo(DataNodeInfo dnInfo, long lba, long addr, int length, int lkey){
		this.dnInfo = dnInfo;
		this.lba = lba;
		this.addr = addr;
		this.length = length;
		this.lkey = lkey;
	}
	
	public void setBlockInfo(BlockInfo blockInfo) {
		this.dnInfo.setDataNodeInfo(blockInfo.getDnInfo());
		this.lba = blockInfo.getLba();
		this.addr = blockInfo.getAddr();
		this.length = blockInfo.getLength();
		this.lkey = blockInfo.getLkey();
		
	}

	public int write(ByteBuffer buffer){
		dnInfo.write(buffer);
		buffer.putLong(lba);
		buffer.putLong(addr);
		buffer.putInt(length);
		buffer.putInt(lkey);
		return CSIZE;
	}
	
	public void update(ByteBuffer buffer) throws UnknownHostException{
		this.dnInfo.update(buffer);
		this.lba = buffer.getLong();
		this.addr = buffer.getLong();
		this.length = buffer.getInt();
		this.lkey = buffer.getInt();
	}

	public long getLba() {
		return lba;
	}

	public long getAddr() {
		return addr;
	}

	public int getLength() {
		return length;
	}

	public int getLkey() {
		return lkey;
	}

	public DataNodeInfo getDnInfo() {
		return dnInfo;
	}
}
