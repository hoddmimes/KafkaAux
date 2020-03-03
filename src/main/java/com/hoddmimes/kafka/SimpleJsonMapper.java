package com.hoddmimes.kafka;


import com.google.gson.*;
import com.google.gson.internal.LinkedHashTreeMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SimpleJsonMapper
{
    private LinkedHashMap<String,Object> mMap;

    public SimpleJsonMapper( LinkedHashMap<String,Object> pMap ) {
        if (pMap == null) {
            mMap = null;
        } else {
            mMap = copyMap(pMap);
        }
    }

    public SimpleJsonMapper(  JsonObject pJsonObject ) {
        if (pJsonObject == null) {
            mMap = new LinkedHashMap<>();
        } else {
            mMap = unpackJson(pJsonObject);
        }
    }

    private LinkedHashMap<String,Object> unpackJson( JsonObject pJsonObject ) {
        LinkedHashMap<String,Object> tMap = new LinkedHashMap<>();
        for( String pKey : pJsonObject.keySet()) {
           JsonElement jElement = pJsonObject.get( pKey );
           mMap.put( pKey, parseJsonObject( jElement));
        }
        return tMap;
    }

    private Object parseJsonObject( JsonElement jElement ) {
        if (jElement.isJsonArray()) {
            List<Object> tList = new ArrayList<>();
            JsonArray jArray = jElement.getAsJsonArray();
            for(int i = 0; i < jArray.size(); i++) {
                tList.add( parseJsonObject(jArray.get(i)));
            }
            return tList;
        }
        if (jElement.isJsonObject()) {
            return unpackJson( jElement.getAsJsonObject());
        }
        if (jElement.isJsonPrimitive()) {
           JsonPrimitive jPrimitive = jElement.getAsJsonPrimitive();
           if (jPrimitive.isBoolean()) {
               return jPrimitive.getAsBoolean();
           }
           if (jPrimitive.isString()) {
               return jPrimitive.getAsString();
           }
           if (jPrimitive.isNumber()) {
              return null;
           }
        }
        return null;
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

    public static void main(String[] args ) {
        String js = "{\"int\" : 123, \"float\": 3.14, \"long\" :1234567891234, \"string\" : \"hej hopp string\",\"boolean\" : true }";
        JsonObject jObj = new JsonParser().parse( js ).getAsJsonObject();
        for(String k : jObj.keySet()) {

            Object o = jObj.get(k);
            if (o instanceof Integer) {
                System.out.println("Integer");
            }
            if (o instanceof Long) {
                System.out.println("Long");
            }
            if (o instanceof String) {
                System.out.println("String");
            }
            if (o instanceof Boolean) {
                System.out.println("Boolean");
            }
            if (o instanceof Double) {
                System.out.println("Boolean");
            }
        }
    }
}
