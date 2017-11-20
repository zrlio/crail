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

import java.util.Iterator;

import com.ibm.crail.CrailContainer;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailMultiFile;
import com.ibm.crail.CrailTable;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.FileInfo;

class CoreDirectory extends CoreNode implements CrailContainer, CrailDirectory, CrailMultiFile, CrailTable {
	
	public CoreDirectory(CoreFileSystem fs, FileInfo fileInfo, String path){
		super(fs, fileInfo, path);
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
	}	

	@Override
	public int files() {
		return (int) fileInfo.getCapacity()/CrailConstants.DIRECTORY_RECORD;
	}

	@Override
	public Iterator<String> listEntries() throws Exception {
		return fs.listEntries(path);
	}

	@Override
	public CoreDirectory asDirectory() throws Exception {
		if (!getType().isDirectory()){
			throw new Exception("file type mismatch, type " + getType());
		}
		return this;
	}
	
	@Override
	public CoreDirectory asContainer() throws Exception {
		if (!getType().isContainer()){
			throw new Exception("file type mismatch, type " + getType());
		}
		return this;
	}	
	
	@Override
	public CrailMultiFile asMultiFile() throws Exception {
		if (!getType().isMultiFile()){
			throw new Exception("file type mismatch, type " + getType());
		}
		return this;
	}	
	
	@Override
	public CrailTable asTable() throws Exception {
		if (!getType().isTable()){
			throw new Exception("file type mismatch, type " + getType());
		}
		return this;
	}
	
	DirectoryOutputStream getDirectoryOutputStream() throws Exception {
		CoreOutputStream outputStream = super.getOutputStream(0);
		return new DirectoryOutputStream(outputStream);
	}
	
	DirectoryInputStream getDirectoryInputStream(boolean randomize) throws Exception {
		CoreInputStream inputStream = super.getInputStream(0);
		return new DirectoryInputStream(inputStream, randomize);
	}	
}

