package graphql.execution.defer;

import graphql.Directives;
import graphql.ExecutionResult;
import graphql.Internal;
import graphql.language.Field;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * This provides support for @defer directives on fields that mean that results will be sent AFTER
 * the main result is sent via a Publisher stream.
 */
@Internal
public class DeferSupport {

    private final AtomicBoolean deferDetected = new AtomicBoolean(false);
    private final Deque<DeferredCall> deferredCalls = new ConcurrentLinkedDeque<>();

    public boolean checkForDeferDirective(List<Field> currentField) {
        for (Field field : currentField) {
            if (field.getDirective(Directives.DeferDirective.getName()) != null) {
                return true;
            }
        }
        return false;
    }

    public void enqueue(DeferredCall deferredCall) {
        deferDetected.set(true);
        deferredCalls.offer(deferredCall);
    }

    public boolean isDeferDetected() {
        return deferDetected.get();
    }

    /**
     * When this is called the deferred execution will begin
     *
     * @return the publisher of deferred results
     */
    public Publisher<ExecutionResult> startDeferredCalls() {
        return Flux.<Mono<ExecutionResult>>generate(sink -> {
            if (deferredCalls.isEmpty()) {
                sink.complete();
            } else {
                sink.next(deferredCalls.pop().invoke());
            }
        })
                   .concatMap(Function.identity());
    }
}
