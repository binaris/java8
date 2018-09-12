package com.binaris;

import java.util.Map;

public class BinarisRequest {
    public String body;
    public String method;
    public String path;
    public Map<String, String> headers;
    public Map<String, String[]> query;
    public String id;
}

