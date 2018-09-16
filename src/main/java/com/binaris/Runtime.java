package com.binaris;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static spark.Spark.*;

class Health {
    public int concurrency;
    public int request_count;
}

class Invoker {
    private String entryPoint;

    public Invoker(String entryPoint) {
        this.entryPoint = entryPoint;
    }

    public JsonElement tryDeserialize(String body) {
        try {
            return (new Gson()).fromJson(body, JsonElement.class);
        } catch (JsonParseException e) {
            return null;
        }
    }

    private static String getStackTrace(Exception exception) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer );
        exception.printStackTrace( printWriter );
        printWriter.flush();
        return writer.toString();
    }

    public Object invoke(spark.Request req, spark.Response res) {
        res.type("application/json");
        Map<String, Object> ret = new HashMap<String, Object>();

        String id = req.headers("x-binaris-request-id");
        if (id == null) {
            res.status(500);
            ret.put("errorCode", "ERR_NO_REQ_ID");
            return ret;
        }

        BinarisRequest bReq = new BinarisRequest();
        bReq.id = id;
        bReq.body = req.body();
        bReq.method = req.requestMethod();
        bReq.path = req.pathInfo().replaceFirst("^/v1/run", "");
        bReq.headers = new HashMap<String, String>();
        for (String header: req.headers()) {
            bReq.headers.put(header, req.headers(header));
        }
        bReq.query = req.queryMap().toMap();

        JsonElement body = tryDeserialize(req.body());

        try {
            long startTime = System.nanoTime();
            Class userCode = Class.forName(entryPoint);
            BinarisFunction userWrapper = (BinarisFunction)userCode.newInstance();
            Object userResponse = userWrapper.handle(body, bReq);
            long elapsed = System.nanoTime() - startTime;
            res.header("x-binaris-bolt-duration-usecs", Long.toString(elapsed / 1000));
            return userResponse;
        } catch (Exception e) {
            res.status(500);
            ret.put("stackTrace", getStackTrace(e));
            ret.put("detailMessage", e.toString());
            return ret;
        } finally {
            res.header("x-binaris-request-id", id);
        }
    }
}

class LogRecord {
    public boolean isErr;
    public String reqid;
    public String msg;
}

class JsonLogStream extends PrintStream {
    private FileOutputStream output;
    private boolean err;
    public JsonLogStream(FileOutputStream output, boolean err) {
        super(output);
        this.output = output;
        this.err = err;
    }

    public void write(byte buf[], int off, int len) {
        LogRecord record = new LogRecord();
        record.msg = new String(buf, off, len);
        record.isErr = this.err;
        record.reqid = "unknown";
        String wrapped = (new Gson()).toJson(record);
        byte[] b = wrapped.getBytes(Charset.forName("UTF-8"));
        super.write(b, 0, b.length);
        super.write('\n');
    }
}

public class Runtime {
    private static AtomicInteger concurrency = new AtomicInteger(0);
    private static AtomicInteger request_count = new AtomicInteger(0);
    private static String funcName;
    private static String entryPoint;
    private static String logDir;

    private static String requiredEnvar(String name) {
        return requiredEnvar(name, null);
    }

    private static String requiredEnvar(String name, String def) {
        String value = System.getenv(name);
        if (value == null) {
            if (def == null) {
                throw new RuntimeException(String.format("Expecting envar %s", name));
            }
            return def;
        }
        return value;
    }

    private static String functionName;

    private static void configure() {
        port(Integer.parseInt(requiredEnvar("BOLT_PORT", "80")));
        funcName = requiredEnvar("BN_FUNCTION");
        entryPoint = requiredEnvar("BN_ENTRYPOINT");
        logDir = requiredEnvar("BN_LOGDIR", "/logs");
    }

    private static boolean hookStdStreams() {
        Path path = Paths.get(logDir, "std.log");
        try {

            FileOutputStream f = new FileOutputStream(path.toString());
            System.setOut(new JsonLogStream(f, false));
            System.setErr(new JsonLogStream(f, true));
            return true;
        } catch (FileNotFoundException e) {
            System.err.println("/logs directory does not exist");
            System.err.println(e.toString());
            return false;
        }
    }

    public static void main(String[] args) {
        configure();
        if (!hookStdStreams()) {
            return;
        }
        spark.Route route = (req, res) -> {
            request_count.incrementAndGet();
            concurrency.incrementAndGet();
            try {
                Invoker invoker = new Invoker(entryPoint);
                Object ret = invoker.invoke(req, res);
                return (new Gson()).toJson(ret);
            } finally {
                concurrency.decrementAndGet();
            }
        };
        get("/v1/run", route);
        post("/v1/run", route);
        put("/v1/run", route);
        delete("/v1/run", route);
        get("/v1/run/*", route);
        post("/v1/run/*", route);
        put("/v1/run/*", route);
        delete("/v1/run/*", route);
        get("/_healthy", (req, res) -> {
            res.type("application/json");
            Health health = new Health();
            health.concurrency = concurrency.get();
            health.request_count = request_count.get();
            return (new Gson()).toJson(health);
        });
    }
}
