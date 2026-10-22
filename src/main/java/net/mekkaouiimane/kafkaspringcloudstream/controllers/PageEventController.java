package net.mekkaouiimane.kafkaspringcloudstream.controllers;

import net.mekkaouiimane.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {
    @Autowired
    private StreamBridge streamBridge;
    @Autowired
    private InteractiveQueryService interactiveQueryService;
    @GetMapping("/publish")
    public PageEvent publish(String name, String topic){
        PageEvent event=new PageEvent(
                name,
                Math.random()>0.5?"U1":"U2",
                new Date(),
                new Random().nextInt(10000)
        );
        streamBridge.send(topic,event);
        return event;
    }
    @GetMapping(value = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1))
                .onBackpressureDrop()
                .map(tick -> {
                    Map<String, Long> result = new HashMap<>();
                    try {
                        ReadOnlyWindowStore<String, Long> store = interactiveQueryService
                                .getQueryableStore("count-store", QueryableStoreTypes.windowStore());
                        Instant now = Instant.now();
                        try (var it = store.fetchAll(now.minusSeconds(5), now)) {   // closes the iterator
                            while (it.hasNext()) {
                                var kv = it.next();
                                result.merge(kv.key.key(), kv.value, Long::sum);
                            }
                        }
                    } catch (InvalidStateStoreException e) {
                        // store not ready yet (startup/rebalance): return an empty map
                    }
                    return result;
                })
                .onErrorContinue((e, o) -> {});
    }
}
