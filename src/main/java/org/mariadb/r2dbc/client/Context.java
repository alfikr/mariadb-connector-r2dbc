/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.client;

public class Context {

  private final long threadId;
  private final long serverCapabilities;
  private short serverStatus;
  private ServerVersion version;

  public Context(
      String serverVersion,
      long threadId,
      long capabilities,
      short serverStatus,
      boolean mariaDBServer) {

    this.threadId = threadId;
    this.serverCapabilities = capabilities;
    this.serverStatus = serverStatus;
    this.version = new ServerVersion(serverVersion, mariaDBServer);
  }

  public long getThreadId() {
    return threadId;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public void setServerStatus(short serverStatus) {
    this.serverStatus = serverStatus;
  }

  public ServerVersion getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "ConnectionContext{" + "threadId=" + threadId + ", version=" + version + '}';
  }
}
