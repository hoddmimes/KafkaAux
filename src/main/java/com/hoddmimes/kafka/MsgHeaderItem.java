package com.hoddmimes.kafka;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.header.Header;


public class MsgHeaderItem implements Header {
    public static final String KEY = "msghdr";
    private final String mKey;
    private final JsonObject mValue;

    public MsgHeaderItem(JsonObject pValue ) {
        mKey = KEY;
        mValue = pValue;
    }

    public MsgHeaderItem(byte[] pValue ) {
        mKey = KEY;
        mValue = JsonParser.parseString( new String(pValue)).getAsJsonObject();
    }

    @Override
    public String key() {
        return mKey;
    }

    @Override
    public byte[] value() {
        return mValue.toString().getBytes();
    }

    public JsonObject getMsgHeader() {
        return mValue;
    }
}
