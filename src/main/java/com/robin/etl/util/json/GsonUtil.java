package com.robin.etl.util.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.util.Map;

/**
 * <p>Project:  kafka-tile38-loader</p>
 * <p>
 * <p>Description:com.zhcx.fencing.comm</p>
 * <p>
 * <p>Copyright: Copyright (c) 2018 create at 2018年12月10日</p>
 * <p>
 * <p>Company: zhcx_DEV</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class GsonUtil {
    public static Gson getGson(){
        return new GsonBuilder().registerTypeAdapter(new TypeToken<Map<String,Object>>(){}.getType(), new GsonDoubleToLongDeserializer()).create();
    }

}

