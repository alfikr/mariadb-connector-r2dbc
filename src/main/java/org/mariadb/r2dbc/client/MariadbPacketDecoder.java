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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.util.List;
import java.util.Queue;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.message.server.PrepareResultPacket;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;

public class MariadbPacketDecoder extends ByteToMessageDecoder {

  private final Queue<CmdElement> responseReceivers;
  private final Client client;

  private Context context = null;
  private boolean isMultipart = false;
  private DecoderState state = DecoderState.INIT_HANDSHAKE;
  private CmdElement cmdElement;
  private CompositeByteBuf multipart;
  private long serverCapabilities;
  private int stateCounter = 0;
  private PrepareResultPacket prepare;
  private ColumnDefinitionPacket[] prepareColumns;

  public MariadbPacketDecoder(Queue<CmdElement> responseReceivers, Client client) {
    this.responseReceivers = responseReceivers;
    this.client = client;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
    while (buf.readableBytes() > 4) {
      int length = buf.getUnsignedMediumLE(buf.readerIndex());

      // packet not complete
      if (buf.readableBytes() < length + 4) return;

      // extract packet
      if (length == 0xffffff) {
        // multipart packet
        if (!isMultipart) {
          isMultipart = true;
          multipart = buf.alloc().compositeBuffer();
        }
        buf.skipBytes(4); // skip length + header
        multipart.addComponent(true, buf.readRetainedSlice(length));
        continue;
      }

      // wait for complete packet
      if (isMultipart) {
        // last part of multipart packet
        buf.skipBytes(3); // skip length
        Sequencer sequencer = new Sequencer(buf.readByte());
        multipart.addComponent(true, buf.readRetainedSlice(length));

        handleBuffer(multipart, sequencer);

        multipart.release();
        isMultipart = false;
        continue;
      }

      // create Object from packet
      ByteBuf packet = buf.readRetainedSlice(4 + length);
      packet.skipBytes(3); // skip length
      Sequencer sequencer = new Sequencer(packet.readByte());
      handleBuffer(packet, sequencer);
      packet.release();
    }
  }

  private void handleBuffer(ByteBuf packet, Sequencer sequencer) {
    if (cmdElement == null && !loadNextResponse()) {
      throw new R2dbcNonTransientResourceException(
          String.format(
              "unexpected message received when no command was send: 0x%s",
              BufferUtils.toString(packet)));
    }

    state =
        state.decoder(
            packet.getUnsignedByte(packet.readerIndex()),
            packet.readableBytes(),
            serverCapabilities);
    ServerMessage msg = state.decode(packet, sequencer, this, cmdElement);
    cmdElement.getSink().next(msg);
    if (msg.ending()) {
      if (cmdElement != null) {
        // complete executed only after setting next element.
        CmdElement element = cmdElement;
        loadNextResponse();
        element.getSink().complete();
      }
      client.sendNext();
    } else {
      state = state.next(this);
    }
  }

  public void connectionError(Throwable err) {
    if (cmdElement != null) {
      cmdElement.getSink().error(err);
      cmdElement = null;
      state = null;
    }
  }

  public Context getContext() {
    return context;
  }

  public int getStateCounter() {
    return stateCounter;
  }

  public void setStateCounter(int counter) {
    stateCounter = counter;
  }

  public PrepareResultPacket getPrepare() {
    return prepare;
  }

  public void setPrepare(PrepareResultPacket prepare) {
    this.prepare = prepare;
    this.prepareColumns = new ColumnDefinitionPacket[prepare.getNumColumns()];
  }

  public ColumnDefinitionPacket[] getPrepareColumns() {
    return prepareColumns;
  }

  public ServerPrepareResult endPrepare() {
    // this.prepareColumns = new ColumnDefinitionPacket[prepare.getNumColumns()];
    ServerPrepareResult prepareResult =
        new ServerPrepareResult(
            this.prepare.getStatementId(), this.prepare.getNumParams(), prepareColumns);
    PrepareCache prepareCache = client.getPrepareCache();
    if (prepareCache != null) {
      ServerPrepareResult cached = prepareCache.put(cmdElement.getSql(), prepareResult);
      if (cached != null) {
        // race condition, remove new one to get the one in cache
        prepareResult.decrementUse(client);
        prepareResult = cached;
      }
    }
    this.prepare = null;
    return prepareResult;
  }

  public void decrementStateCounter() {
    stateCounter--;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  private boolean loadNextResponse() {
    this.cmdElement = responseReceivers.poll();
    if (cmdElement != null) {
      state = cmdElement.getInitialState();
      return true;
    }
    state = null;
    return false;
  }

  public void setContext(Context context) {
    this.context = context;
    this.serverCapabilities = this.context.getServerCapabilities();
  }
}
