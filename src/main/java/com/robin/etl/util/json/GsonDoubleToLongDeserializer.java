package com.robin.etl.util.json;

import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>Project:  kafka-tile38-loader</p>
 * <p>
 * <p>Description:com.zhcx.comm</p>
 * <p>
 * <p>Copyright: Copyright (c) 2018 create at 2018年12月13日</p>
 * <p>
 * <p>Company: zhcx_DEV</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class GsonDoubleToLongDeserializer implements JsonDeserializer<Map<String, Object>> {

    @Override  @SuppressWarnings("unchecked")
    public Map<String, Object> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        return (Map<String, Object>) read(json);
    }

    public Object read(JsonElement in) {

        if(in.isJsonArray()){
            List<Object> list = new ArrayList<Object>();
            JsonArray arr = in.getAsJsonArray();
            for (JsonElement anArr : arr) {
                list.add(read(anArr));
            }
            return list;
        }else if(in.isJsonObject()){
            Map<String, Object> map = new LinkedTreeMap<String, Object>();
            JsonObject obj = in.getAsJsonObject();
            Set<Map.Entry<String, JsonElement>> entitySet = obj.entrySet();
            for(Map.Entry<String, JsonElement> entry: entitySet){
                map.put(entry.getKey(), read(entry.getValue()));
            }
            return map;
        }else if( in.isJsonPrimitive()){
            JsonPrimitive prim = in.getAsJsonPrimitive();
            if(prim.isBoolean()){
                return prim.getAsBoolean();
            }else if(prim.isString()){
                return prim.getAsString();
            }else if(prim.isNumber()){
                Number num = prim.getAsNumber();
                if(Math.ceil(num.doubleValue())  == num.longValue()) {
                    Long tval=num.longValue();
                    if(tval<Integer.MAX_VALUE){
                        return num.intValue();
                    }else
                        return tval;
                }
                else{
                    return num.doubleValue();
                }
            }
        }
        return null;
    }
}