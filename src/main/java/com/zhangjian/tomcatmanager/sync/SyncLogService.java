package com.zhangjian.tomcatmanager.sync;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SyncLogService {

    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    public void addEmitter(String connectionId, SseEmitter emitter) {
        emitters.put(connectionId, emitter);
        emitter.onCompletion(() -> emitters.remove(connectionId));
        emitter.onTimeout(() -> emitters.remove(connectionId));
    }

    public void broadcast(String connectionId, String logMessage) {
        SseEmitter emitter = emitters.get(connectionId);
        if (emitter != null) {
            try {
                emitter.send(SseEmitter.event().data(logMessage));
            } catch (IOException e) {
                emitters.remove(connectionId);
            }
        }
    }
}
