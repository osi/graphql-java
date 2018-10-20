package graphql.execution;

import graphql.Internal;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Internal
@SuppressWarnings("FutureReturnValueIgnored")
public class Async {

    public static <U> CompletableFuture<List<U>> each(List<CompletableFuture<U>> futures) {
        CompletableFuture<List<U>> overallResult = new CompletableFuture<>();

        CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((noUsed, exception) -> {
                    if (exception != null) {
                        overallResult.completeExceptionally(exception);
                        return;
                    }
                    List<U> results = new ArrayList<>();
                    for (CompletableFuture<U> future : futures) {
                        results.add(future.join());
                    }
                    overallResult.complete(results);
                });
        return overallResult;
    }

    /**
     * Turns an object T into a Mono if its not already
     *
     * @param t   - the object to check
     * @param <T> for two
     *
     * @return a CompletableFuture
     */
    public static <T> Mono<T> toMono(T t) {
        if (t instanceof Mono) {
            //noinspection unchecked
            return ((Mono<T>) t);
        }
        if (t instanceof CompletionStage) {
            //noinspection unchecked
            return Mono.fromCompletionStage((CompletionStage<T>) t);
        } else {
            return Mono.justOrEmpty(t);
        }
    }

}
