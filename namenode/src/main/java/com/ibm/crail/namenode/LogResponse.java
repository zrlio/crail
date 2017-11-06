package com.ibm.crail.namenode;

import java.nio.ByteBuffer;

import com.ibm.crail.rpc.RpcNameNodeState;
import com.ibm.crail.rpc.RpcProtocol;
import com.ibm.crail.rpc.RpcResponseMessage;

public class LogResponse implements RpcNameNodeState {
	public static final int CSIZE = 4 + Math.max(RpcResponseMessage.GetBlockRes.CSIZE, RpcResponseMessage.RenameRes.CSIZE);
	
	private short type;
	private short error;
	private RpcResponseMessage.VoidRes voidRes;
	private RpcResponseMessage.CreateFileRes createFileRes;
	private RpcResponseMessage.GetFileRes getFileRes;
	private RpcResponseMessage.DeleteFileRes delFileRes;
	private RpcResponseMessage.RenameRes renameRes;
	private RpcResponseMessage.GetBlockRes getBlockRes;
	private RpcResponseMessage.GetLocationRes getLocationRes;	
	private RpcResponseMessage.GetDataNodeRes getDataNodeRes;
	private RpcResponseMessage.PingNameNodeRes pingNameNodeRes;
	
	public LogResponse() {
		this.type = 0;
		this.error = 0;
		
		this.voidRes = new RpcResponseMessage.VoidRes();
		this.createFileRes = new RpcResponseMessage.CreateFileRes();
		this.getFileRes = new RpcResponseMessage.GetFileRes();
		this.delFileRes = new RpcResponseMessage.DeleteFileRes();
		this.renameRes = new RpcResponseMessage.RenameRes();
		this.getBlockRes = new RpcResponseMessage.GetBlockRes();
		this.getLocationRes = new RpcResponseMessage.GetLocationRes();
		this.getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
		this.pingNameNodeRes = new RpcResponseMessage.PingNameNodeRes();
	}
	
	public LogResponse(RpcResponseMessage.VoidRes message) {
		this.type = message.getType();
		this.voidRes = message;
	}
	
	public LogResponse(RpcResponseMessage.CreateFileRes message) {
		this.type = message.getType();
		this.createFileRes = message;
	}	
	
	public LogResponse(RpcResponseMessage.GetFileRes message) {
		this.type = message.getType();
		this.getFileRes = message;
	}
	
	public LogResponse(RpcResponseMessage.DeleteFileRes message) {
		this.type = message.getType();
		this.delFileRes = message;
	}	
	
	public LogResponse(RpcResponseMessage.RenameRes message) {
		this.type = message.getType();
		this.renameRes = message;
	}
	
	public LogResponse(RpcResponseMessage.GetBlockRes message) {
		this.type = message.getType();
		this.getBlockRes = message;
	}
	
	public LogResponse(RpcResponseMessage.GetLocationRes message) {
		this.type = message.getType();
		this.getLocationRes = message;
	}
	
	public LogResponse(RpcResponseMessage.GetDataNodeRes message) {
		this.type = message.getType();
		this.getDataNodeRes = message;
	}	
	
	public LogResponse(RpcResponseMessage.PingNameNodeRes message) {
		this.type = message.getType();
		this.pingNameNodeRes = message;
	}
	
	public void setType(short type) throws Exception {
		this.type = type;
		switch(type){
		case RpcProtocol.RES_VOID:
			if (voidRes == null){
				throw new Exception("Response type not set");
			}
			break;
		case RpcProtocol.RES_CREATE_FILE:
			if (createFileRes == null){
				throw new Exception("Response type not set");
			}
			break;			
		case RpcProtocol.RES_GET_FILE:
			if (getFileRes == null){
				throw new Exception("Response type not set");
			}
			break;
		case RpcProtocol.RES_DELETE_FILE:
			if (delFileRes == null){
				throw new Exception("Response type not set");
			}
			break;			
		case RpcProtocol.RES_RENAME_FILE:
			if (renameRes == null){
				throw new Exception("Response type not set");
			}
			break;			
		case RpcProtocol.RES_GET_BLOCK:
			if (getBlockRes == null){
				throw new Exception("Response type not set");
			}
			break;
		case RpcProtocol.RES_GET_LOCATION:
			if (getLocationRes == null){
				throw new Exception("Response type not set");
			}
			break;			
		case RpcProtocol.RES_GET_DATANODE:
			if (getDataNodeRes == null){
				throw new Exception("Response type not set");
			}
			break;			
		case RpcProtocol.RES_PING_NAMENODE:
			if (pingNameNodeRes == null){
				throw new Exception("Response type not set");
			}
			break;
		}		
	}	

	public int size(){
		return CSIZE;
	}
	
	public int write(ByteBuffer buffer){
		buffer.putShort(type);
		buffer.putShort(error);
		
		int written = 4;
		switch(type){
		case RpcProtocol.RES_VOID:
			written += voidRes.write(buffer);
			break;	
		case RpcProtocol.RES_CREATE_FILE:
			written += createFileRes.write(buffer);
			break;				
		case RpcProtocol.RES_GET_FILE:
			written += getFileRes.write(buffer);
			break;
		case RpcProtocol.RES_DELETE_FILE:
			written += delFileRes.write(buffer);
			break;				
		case RpcProtocol.RES_RENAME_FILE:
			written += renameRes.write(buffer);
			break;				
		case RpcProtocol.RES_GET_BLOCK:
			written += getBlockRes.write(buffer);
			break;
		case RpcProtocol.RES_GET_LOCATION:
			written += getLocationRes.write(buffer);
			break;			
		case RpcProtocol.RES_GET_DATANODE:
			written += getDataNodeRes.write(buffer);
			break;			
		case RpcProtocol.RES_PING_NAMENODE:
			written += pingNameNodeRes.write(buffer);
			break;			
		}
		
		return written;
	}
	
	public void update(ByteBuffer buffer){
		this.type = buffer.getShort();
		this.error = buffer.getShort();
		
		switch(type){
		case RpcProtocol.RES_VOID:
			voidRes.update(buffer);
			voidRes.setError(error);
			break;			
		case RpcProtocol.RES_CREATE_FILE:
			createFileRes.update(buffer);
			createFileRes.setError(error);
			break;				
		case RpcProtocol.RES_GET_FILE:
			getFileRes.update(buffer);
			getFileRes.setError(error);
			break;	
		case RpcProtocol.RES_DELETE_FILE:
			delFileRes.update(buffer);
			delFileRes.setError(error);
			break;				
		case RpcProtocol.RES_RENAME_FILE:
			renameRes.update(buffer);
			renameRes.setError(error);
			break;				
		case RpcProtocol.RES_GET_BLOCK:
			getBlockRes.update(buffer);
			getBlockRes.setError(error);
			break;
		case RpcProtocol.RES_GET_LOCATION:
			getLocationRes.update(buffer);
			getLocationRes.setError(error);
			break;			
		case RpcProtocol.RES_GET_DATANODE:
			getDataNodeRes.update(buffer);
			getDataNodeRes.setError(error);
			break;			
		case RpcProtocol.RES_PING_NAMENODE:
			pingNameNodeRes.update(buffer);
			pingNameNodeRes.setError(error);
			break;		
		}
	}
	
	public short getType(){
		return type;
	}

	public short getError() {
		return error;
	}

	public void setError(short error) {
		this.error = error;
	}	
	
	public RpcResponseMessage.VoidRes getVoid() {
		return voidRes;
	}	
	
	public RpcResponseMessage.CreateFileRes createFile() {
		return createFileRes;
	}	
	
	public RpcResponseMessage.GetFileRes getFile() {
		return getFileRes;
	}
	
	public RpcResponseMessage.DeleteFileRes delFile() {
		return delFileRes;
	}	
	
	public RpcResponseMessage.RenameRes getRename() {
		return renameRes;
	}	

	public RpcResponseMessage.GetBlockRes getBlock() {
		return getBlockRes;
	}	
	
	public RpcResponseMessage.GetLocationRes getLocation() {
		return getLocationRes;
	}	
	
	public RpcResponseMessage.GetDataNodeRes getDataNode() {
		return getDataNodeRes;
	}	
	
	public RpcResponseMessage.PingNameNodeRes pingNameNode(){
		return this.pingNameNodeRes;
	}
}
