package com.ibm.crail.namenode;

import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcNameNodeState;
import com.ibm.crail.rpc.RpcProtocol;
import com.ibm.crail.rpc.RpcRequestMessage.CreateFileReq;
import com.ibm.crail.rpc.RpcRequestMessage.DumpNameNodeReq;
import com.ibm.crail.rpc.RpcRequestMessage.GetBlockReq;
import com.ibm.crail.rpc.RpcRequestMessage.GetDataNodeReq;
import com.ibm.crail.rpc.RpcRequestMessage.GetFileReq;
import com.ibm.crail.rpc.RpcRequestMessage.GetLocationReq;
import com.ibm.crail.rpc.RpcRequestMessage.PingNameNodeReq;
import com.ibm.crail.rpc.RpcRequestMessage.RemoveFileReq;
import com.ibm.crail.rpc.RpcRequestMessage.RenameFileReq;
import com.ibm.crail.rpc.RpcRequestMessage.SetBlockReq;
import com.ibm.crail.rpc.RpcRequestMessage.SetFileReq;
import com.ibm.crail.rpc.RpcResponseMessage.CreateFileRes;
import com.ibm.crail.rpc.RpcResponseMessage.DeleteFileRes;
import com.ibm.crail.rpc.RpcResponseMessage.GetBlockRes;
import com.ibm.crail.rpc.RpcResponseMessage.GetDataNodeRes;
import com.ibm.crail.rpc.RpcResponseMessage.GetFileRes;
import com.ibm.crail.rpc.RpcResponseMessage.GetLocationRes;
import com.ibm.crail.rpc.RpcResponseMessage.PingNameNodeRes;
import com.ibm.crail.rpc.RpcResponseMessage.RenameRes;
import com.ibm.crail.rpc.RpcResponseMessage.VoidRes;

public class LogDispatcher implements RpcNameNodeService {
	private RpcNameNodeService service;
	private LogService logService;
	
	public LogDispatcher(RpcNameNodeService service) throws Exception{
		this.service = service;
		this.logService = new LogService();
		this.logService.replay(service);
	}

	@Override
	public short createFile(CreateFileReq request, CreateFileRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_CREATE_FILE);
		logService.writeRecord(record);
		return service.createFile(request, response, errorState);
	}

	@Override
	public short getFile(GetFileReq request, GetFileRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.getFile(request, response, errorState);
	}

	@Override
	public short setFile(SetFileReq request, VoidRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_SET_FILE);
		logService.writeRecord(record);		
		return service.setFile(request, response, errorState);
	}

	@Override
	public short removeFile(RemoveFileReq request, DeleteFileRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_REMOVE_FILE);
		logService.writeRecord(record);		
		return service.removeFile(request, response, errorState);
	}

	@Override
	public short renameFile(RenameFileReq request, RenameRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_RENAME_FILE);
		logService.writeRecord(record);		
		return service.renameFile(request, response, errorState);
	}

	@Override
	public short getDataNode(GetDataNodeReq request, GetDataNodeRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.getDataNode(request, response, errorState);
	}

	@Override
	public short setBlock(SetBlockReq request, VoidRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_SET_BLOCK);
		logService.writeRecord(record);		
		return service.setBlock(request, response, errorState);
	}

	@Override
	public short getBlock(GetBlockReq request, GetBlockRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_GET_BLOCK);
		logService.writeRecord(record);		
		return service.getBlock(request, response, errorState);
	}

	@Override
	public short getLocation(GetLocationReq request, GetLocationRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.getLocation(request, response, errorState);
	}

	@Override
	public short dump(DumpNameNodeReq request, VoidRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.dump(request, response, errorState);
	}

	@Override
	public short ping(PingNameNodeReq request, PingNameNodeRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.ping(request, response, errorState);
	}
	
}
