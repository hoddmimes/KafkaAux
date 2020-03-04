package com.hoddmimes.kafka;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.header.Header;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class MsgHeader extends LinkedHashMap<String, MsgHeader.Item> implements Iterable<Header>
{

    public MsgHeader() {
        super();
    }

    public void add( String pKey, String pValue  )
    {
        this.put( pKey, new Item( pKey, pValue ));
    }

    public void add( String pKey, Long pValue  )
    {
        this.put( pKey, new Item( pKey, pValue ));
    }

    public void add( String pKey, Double pValue  )
    {
        this.put( pKey, new Item( pKey, pValue ));
    }

    public void add( String pKey, Integer pValue  )
    {
        this.put( pKey, new Item( pKey, pValue ));
    }

    public void add( String pKey, byte[] pValue  )
    {
        this.put( pKey, new Item( pKey, pValue ));
    }


    @Override
    public Iterator<Header> iterator() {
        return this.iterator();
    }

    public static class Item implements Header
    {
        private String mKey;
        private Object mValue;

        public Item( String pKey, Object pValue) {
            mKey = pKey;
            mValue = pValue;
        }


        @Override
        public String key() {
            return mKey;
        }

        @Override
        public byte[] value() {
            try {
                return serializeValue();
            }
            catch( IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        private byte[] serializeValue() throws IOException {
            ByteArrayOutputStream boas = new ByteArrayOutputStream(256);
            ObjectOutputStream oos = new ObjectOutputStream( boas );

            if (mValue == null) {
                return null;
            } else if ( mValue instanceof String) {
                oos.writeBytes((String) mValue);
            } else if ( mValue instanceof Integer) {
                oos.writeInt((int) mValue);
            } else if ( mValue instanceof Double) {
                oos.writeDouble((double) mValue);
            } else if ( mValue instanceof Long) {
                oos.writeLong((long) mValue);
            } else if ( mValue instanceof byte[]) {
                oos.write((byte[]) mValue );
            } else {
                throw new IOException("Unknown MsgHeader Type (" + mValue.getClass().getSimpleName() + ")");
            }

            oos.flush();
            return boas.toByteArray();
        }


    }

}
