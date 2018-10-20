package graphql.execution;

import graphql.Assert;
import graphql.Internal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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

    public static <T, U> Flux<U> each(Iterable<T> list, BiFunction<Long, T, Mono<U>> cfFactory) {
        return Flux.fromIterable(list)
                   .index(cfFactory)
                   .flatMapSequential(Function.identity());
    }

    public static <T, U> CompletableFuture<List<U>> eachSequentially(Iterable<T> list, CFFactory<T, U> cfFactory) {
        CompletableFuture<List<U>> result = new CompletableFuture<>();
        eachSequentiallyImpl(list.iterator(), cfFactory, 0, new ArrayList<>(), result);
        return result;
    }

    private static <T, U> void eachSequentiallyImpl(Iterator<T> iterator, CFFactory<T, U> cfFactory, int index, List<U> tmpResult, CompletableFuture<List<U>> overallResult) {
        if (!iterator.hasNext()) {
            overallResult.complete(tmpResult);
            return;
        }
        CompletableFuture<U> cf;
        try {
            cf = cfFactory.apply(iterator.next(), index, tmpResult);
            Assert.assertNotNull(cf, "cfFactory must return a non null value");
        } catch (Exception e) {
            cf = new CompletableFuture<>();
            cf.completeExceptionally(new CompletionException(e));
        }
        cf.whenComplete((cfResult, exception) -> {
            if (exception != null) {
                overallResult.completeExceptionally(exception);
                return;
            }
            tmpResult.add(cfResult);
            eachSequentiallyImpl(iterator, cfFactory, index + 1, tmpResult, overallResult);
        });
    }

    /**
     * Turns an object T into a CompletableFuture if its not already
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

    public static <T> CompletableFuture<T> tryCatch(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            CompletableFuture<T> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }
    }

    public static <T> CompletableFuture<T> exceptionallyCompletedFuture(Throwable exception) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }

    public static <T> void copyResults(CompletableFuture<T> source, CompletableFuture<T> target) {
        source.whenComplete((o, throwable) -> {
            if (throwable != null) {
                target.completeExceptionally(throwable);
                return;
            }
            target.complete(o);
        });
    }

    @FunctionalInterface
    public interface CFFactory<T, U> {
        CompletableFuture<U> apply(T input, int index, List<U> previousResults);
    }

}
