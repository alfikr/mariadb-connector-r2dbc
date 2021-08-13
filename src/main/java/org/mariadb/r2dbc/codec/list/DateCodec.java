package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.EnumSet;

import static org.mariadb.r2dbc.codec.list.LocalDateCodec.parseDate;

public class DateCodec implements Codec<Date> {
    private static final EnumSet<DataType> COMPATIBLE_TYPES =
            EnumSet.of(
                    DataType.DATETIME,
                    DataType.TIMESTAMP,
                    DataType.DATE,
                    DataType.NEWDATE
            );
    @Override
    public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
        return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Date.class);
    }

    @Override
    public boolean canEncode(Class<?> value) {
        return Date.class.isAssignableFrom(value);
    }

    @Override
    public Date decodeText(ByteBuf buffer, int length, ColumnDefinitionPacket column, Class<? extends Date> type) {
        int[] parts;
        switch (column.getType()){
            case YEAR:
                short y = (short) LongCodec.parse(buffer,length);
                if(length==2 && column.getLength()==2){
                    if(y<=69){
                        y+=2000;
                    }else {
                        y+=1900;
                    }
                }
                return Date.from(LocalDate.of(y,1,1).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
            case NEWDATE:
            case DATE:
                parts=parseDate(buffer,length);
                break;
            default:
                parts=LocalDateTimeCodec.parseTimestamp(buffer.readCharSequence(length, StandardCharsets.US_ASCII).toString());
                break;
        }
        if (parts==null) return null;
        return Date.from(LocalDate.of(parts[0],parts[1],parts[2] )
                .atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    @Override
    public void encodeText(ByteBuf buf, Context context, Date value) {

    }

    @Override
    public Date decodeBinary(ByteBuf buffer, int length, ColumnDefinitionPacket column, Class<? extends Date> type) {
        return null;
    }

    @Override
    public void encodeBinary(ByteBuf buf, Context context, Date value) {

    }

    @Override
    public DataType getBinaryEncodeType() {
        return null;
    }
}
