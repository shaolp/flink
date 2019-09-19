/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherFactoryServices;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * Factory for the {@link DispatcherLeaderProcessImplFactory}.
 */
public class SessionDispatcherLeaderProcessFactoryFactory implements DispatcherLeaderProcessFactoryFactory {

	private final DispatcherFactory dispatcherFactory;

	private SessionDispatcherLeaderProcessFactoryFactory(DispatcherFactory dispatcherFactory) {
		this.dispatcherFactory = dispatcherFactory;
	}

	@Override
	public DispatcherLeaderProcessFactory createFactory(
			JobGraphStoreFactory jobGraphStoreFactory,
			Executor ioExecutor,
			RpcService rpcService,
			PartialDispatcherFactoryServices dispatcherFactoryServices,
			FatalErrorHandler fatalErrorHandler) {
		final DispatcherLeaderProcessImpl.DispatcherServiceFactory dispatcherServiceFactory = new DispatcherServiceImplFactory(
			dispatcherFactory,
			rpcService,
			dispatcherFactoryServices);

		return new DispatcherLeaderProcessImplFactory(
			dispatcherServiceFactory,
			jobGraphStoreFactory,
			ioExecutor,
			fatalErrorHandler);
	}

	public static SessionDispatcherLeaderProcessFactoryFactory create(DispatcherFactory dispatcherFactory) {
		return new SessionDispatcherLeaderProcessFactoryFactory(dispatcherFactory);
	}
}
