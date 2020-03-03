package com.hoddmimes.kafka;


import com.google.gson.*;
import com.google.gson.internal.LinkedHashTreeMap;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SimpleJsonMapper extends LinkedHashMap<String,Object>
{

    public SimpleJsonMapper( LinkedHashMap<String,Object> pMap ) {
        super();
        this.putAll( copyMap( pMap ) );
    }

    public SimpleJsonMapper(  JsonObject pJsonObject ) {
        super();
        this.putAll(unpackJson(pJsonObject));
    }

    private LinkedHashMap<String,Object> unpackJson( JsonObject pJsonObject ) {
        LinkedHashMap<String,Object> tMap = new LinkedHashMap<>();
        for( String pKey : pJsonObject.keySet()) {
           JsonElement jElement = pJsonObject.get( pKey );
           tMap.put( pKey, parseJsonObject( jElement));
        }
        return tMap;
    }

    private Object parseJsonObject( JsonElement jElement ) {
        // Array ?
        if (jElement.isJsonArray()) {
            List<Object> tList = new ArrayList<>();
            JsonArray jArray = jElement.getAsJsonArray();
            for(int i = 0; i < jArray.size(); i++) {
                tList.add( parseJsonObject(jArray.get(i)));
            }
            return tList;
        }
        // Object ?
        if (jElement.isJsonObject()) {
            return unpackJson( jElement.getAsJsonObject());
        }
        // Primitive ?
        if (jElement.isJsonPrimitive()) {
           JsonPrimitive jPrimitive = jElement.getAsJsonPrimitive();
           if (jPrimitive.isBoolean()) {
               return jPrimitive.getAsBoolean();
           }
            if (jPrimitive.isNumber()) {
                return parseJsonNumber( jPrimitive );
            }
           if (jPrimitive.isString()) {
               return jPrimitive.getAsString();
           }
        }
        return null;
    }

    public Map<String, Object> asMap() {
        return this;
    }

    private JsonElement toJsonPrimitive( Object pObject ) {
        if (pObject instanceof Boolean) {
            return new JsonPrimitive( (Boolean) pObject );
        }
        if (pObject instanceof Double) {
            return new JsonPrimitive( (Double) pObject );
        }
        if (pObject instanceof Long) {
            return new JsonPrimitive( (Long) pObject );
        }
        if (pObject instanceof Integer) {
            return new JsonPrimitive( (Integer) pObject );
        }
        return new JsonPrimitive( (String) pObject.toString());
    }

    private JsonPrimitive toJsonArray( List<Object> pList) {
        JsonArray jArray = new JsonArray();
        for( Object o : pList ) {
           jArray.add( toJsonElement( 0));
        }
        return jArray.getAsJsonPrimitive();
    }

    public  JsonObject asJson() {
        return toJsonObject( this );
    }

    private  JsonElement toJsonElement( Object pObject  ) {
        if (pObject instanceof List) {
            return toJsonArray((List) pObject);
        }
        if (pObject instanceof Map) {
            return toJsonObject((Map) pObject);
        }
        return toJsonPrimitive(pObject);
    }

    private  JsonObject toJsonObject( Map<String,Object> pMap )
    {
        JsonObject jObj = new JsonObject();
        for( Map.Entry<String,Object> tItem : pMap.entrySet() ) {
           jObj.add(  tItem.getKey(), toJsonElement(tItem.getValue()));
        }
        return jObj;
    }


    private LinkedHashMap<String,Object> copyMap( Map<String, ? extends Object> pMap ) {
        if (pMap == null) {
            return null;
        }
        LinkedHashMap<String,Object> tMap = new LinkedHashMap<>();
        for( String tKey : pMap.keySet()) {
            Object tValue = pMap.get( tKey );
            if (tValue instanceof Map) {
                tMap.put( tKey, copyMap((Map<String, ? extends Object>) tValue));
            } else {
                tMap.put( tKey, tValue);
            }
        }
        return tMap;
    }


    public static Object parseJsonNumber( JsonPrimitive jNumberPrimitive ) {
        String jNumberStr = jNumberPrimitive.getAsString();
        // Double ?
        if (jNumberStr.contains(".")) {
            return Double.parseDouble(jNumberStr);
        }
        // HEX string ?
        if (jNumberStr.toLowerCase().startsWith("0x")) {
            try {
                return Integer.parseInt(jNumberStr, 16);
            } catch (NumberFormatException e) {
                return Long.parseLong(jNumberStr, 16);
            }
        }
        // Plain integer or Long
        try {
            return Integer.parseInt(jNumberStr);
        } catch (NumberFormatException e) {
            return Long.parseLong(jNumberStr);
        }
    }




    public static void main(String[] args ) {
        String js = "{\"int\" : 123, \"float\": 3.14, \"long\" :1234567891234, \"string\" : \"hej hopp string\",\"boolean\" : true }";
        JsonObject jObj = new JsonParser().parse( js ).getAsJsonObject();
            JsonPrimitive p = jObj.get("long").getAsJsonPrimitive();
            Object o = parseJsonNumber( p );

    }
}
