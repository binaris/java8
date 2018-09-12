package com.binaris;
import com.google.gson.JsonElement;

public interface BinarisFunction {
    public Object handle(JsonElement body, BinarisRequest request) throws BinarisException;
}

