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

package com.ibm.crail.core;

import java.util.concurrent.Semaphore;

import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailContainer;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailKeyValue;
import com.ibm.crail.CrailMultiFile;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.CrailTable;
import com.ibm.crail.metadata.FileInfo;

public class CoreFile extends CoreNode implements CrailFile, CrailKeyValue {
	private Semaphore outputStreams;
	
	public CoreFile(CoreFileSystem fs, FileInfo fileInfo, String path){
		super(fs, fileInfo, path);
		this.outputStreams = new Semaphore(1);
	}
	
	public CrailInputStream getDirectInputStream(long readHint) throws Exception{
		if (fileInfo.getType().isDirectory()){
			throw new Exception("Cannot open stream for directory");
		}		
		
		return super.getInputStream(readHint);
	}	
	
	public synchronized CrailOutputStream getDirectOutputStream(long writeHint) throws Exception {
		if (fileInfo.getType().isDirectory()){
			throw new Exception("Cannot open stream for directory");
		}		
		if (fileInfo.getToken() == 0){
			throw new Exception("File is in read mode, cannot create outputstream, fd " + fileInfo.getFd());
		}
		if (!outputStreams.tryAcquire()){
			throw new Exception("Only one concurrent output stream per file allowed");
		}
		return super.getOutputStream(writeHint);
	}
	
	public long getToken() {
		return fileInfo.getToken();
	}

	public boolean tokenFree(){
		return fileInfo.tokenFree();
	}

	public CoreFile asFile() throws Exception {
		if (!getType().isDataFile()){
			throw new Exception("file type mismatch, type " + getType());
		}
		return this;
	}
	
	public CoreFile asKeyValue() throws Exception {
		if (!getType().isKeyValue()){
			throw new Exception("file type mismatch, type " + getType());
		}		
		return this;
	}	

	void closeOutputStream(CoreOutputStream stream) throws Exception {
		super.closeOutputStream(stream);
		outputStreams.release();
	}
}

class CoreEarlyFile implements CrailFile, CrailKeyValue {
	private CoreFileSystem fs;
	private String path;
	private CrailNodeType type;
	private CreateNodeFuture future;
	private CrailFile file;
	
	public CoreEarlyFile(CoreFileSystem fs, String path, CrailNodeType type, CreateNodeFuture future) {
		this.fs = fs;
		this.path = path;
		this.type = type;
		this.future = future;
		this.file = null;
	}

	public CrailInputStream getDirectInputStream(long readHint) throws Exception{
		return file().getDirectInputStream(readHint);
	}	
	
	public synchronized CrailOutputStream getDirectOutputStream(long writeHint) throws Exception {
		return file().getDirectOutputStream(writeHint);
	}
	
	public CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception{
		return fs.getBlockLocations(path, start, len);
	}	
	
	public long getToken() {
		return file().getToken();
	}

	public CrailFile asFile() throws Exception {
		return this;
	}
	
	public CrailKeyValue asKeyValue() throws Exception {
		return this;
	}	

	@Override
	public CrailFS getFileSystem() {
		return fs;
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public CrailNode syncDir() throws Exception {
		return file().syncDir();
	}

	@Override
	public long getModificationTime() {
		return file().getModificationTime();
	}

	@Override
	public long getCapacity() {
		return file().getCapacity();
	}

	@Override
	public CrailNodeType getType() {
		return type;
	}
	
	@Override
	public CrailContainer asContainer() throws Exception {
		throw new Exception("this is not a container");
	}	

	@Override
	public CrailDirectory asDirectory() throws Exception {
		throw new Exception("this is not a directory");
	}

	@Override
	public CrailMultiFile asMultiFile() throws Exception {
		throw new Exception("this is not a multifile");
	}
	
	@Override
	public CrailTable asTable() throws Exception {
		throw new Exception("this is not a table");
	}	

	@Override
	public long getFd() {
		return file().getFd();
	}

	private synchronized CrailFile file() {
		try {
			if (file == null){
				file = this.future.get().asFile();
			}
			return file;
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}

