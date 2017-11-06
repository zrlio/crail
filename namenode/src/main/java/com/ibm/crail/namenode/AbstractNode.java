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

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.FileInfo;

public abstract class AbstractNode extends FileInfo implements Delayed {
//	private static AtomicLong fdcount = new AtomicLong(0);
	private int fileComponent;
	private AtomicLong dirOffsetCounter;
	private ConcurrentHashMap<Integer, AbstractNode> children;
	private long delay;
	private int storageClass;
	private int locationClass;
	
	public AbstractNode(long fd, int fileComponent, CrailNodeType type, int storageClass, int locationAffinity){
		super(fd, type);
		
		this.fileComponent = fileComponent;
		this.storageClass = storageClass;
		this.locationClass = locationAffinity;
		this.children = new ConcurrentHashMap<Integer, AbstractNode>();
		this.delay = System.currentTimeMillis();
		this.dirOffsetCounter = new AtomicLong(0);
		this.setModificationTime(System.currentTimeMillis());
	}
	
	boolean addChild(AbstractNode child) throws Exception {
		if (!this.getType().isContainer()){
			return false;
		} 
		
		AbstractNode old = children.putIfAbsent(child.getComponent(), child);
		if (old == null){
			child.setDirOffset(dirOffsetCounter.getAndAdd(CrailConstants.DIRECTORY_RECORD));
			return true;
		} else {
			return false;
		}
	}	

	AbstractNode removeChild(AbstractNode child) {
		child = children.remove(child.getComponent());
		return child;
	}
	
	void rename(int newFileComponent) throws Exception {
		this.fileComponent = newFileComponent;
	}	

	public abstract NameNodeBlockInfo getBlock(int index);

	public abstract boolean addBlock(int index, NameNodeBlockInfo block);
	
	public abstract void freeBlocks(BlockStore blockStore) throws UnknownHostException;
	
	public AbstractNode getChild(int component) {
		return children.get(component);
	}

	public int getComponent() {
		return this.fileComponent;
	}
	
	public Iterator<AbstractNode> childIterator(){
		return children.values().iterator();
	}
	
	boolean hasChildren(){
		return children.size() > 0;
	}
	
	public void dump(){
		System.out.println(this.toString());
		for (AbstractNode child : children.values()){
			child.dump();
		}		
	}
	
	@Override
	public String toString() {
		return String.format("%08d\t%08d\t\t%08d\t\t%08d\t\t%08d", getFd(), fileComponent, getCapacity(), getType().getLabel(), getDirOffset());
	}	

	@Override
	public long getDelay(TimeUnit unit) {
		long diff = delay - System.currentTimeMillis();
		long _delay = unit.convert(diff, TimeUnit.MILLISECONDS);
		return _delay;		
	}

	public void setDelay(long delay) {
		this.delay = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delay);
	}

	@Override
	public int compareTo(Delayed o) {
		return 0;
	}

	public int getStorageClass() {
		return storageClass;
	}

	public int getLocationClass() {
		return locationClass;
	}
}
