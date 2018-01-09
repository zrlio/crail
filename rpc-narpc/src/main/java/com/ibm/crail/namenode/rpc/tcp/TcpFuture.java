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

package com.ibm.crail.namenode.rpc.tcp;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ibm.crail.rpc.RpcFuture;
import com.ibm.narpc.NaRPCFuture;

public class TcpFuture<T> implements RpcFuture<T> {
	private NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future;
	private T response;
	private boolean prefetched;
	
	public TcpFuture(NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future, T resp) {
		this.future = future;
		this.response = resp;
		this.prefetched = false;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return future.isDone();
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		future.get();
		return response;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		future.get(timeout, unit);
		return response;
	}

	@Override
	public int getTicket() {
		return (int) future.getTicket();
	}

	@Override
	public boolean isPrefetched() {
		return prefetched;
	}

	@Override
	public void setPrefetched(boolean prefetched) {
		this.prefetched = prefetched;
	}
}
