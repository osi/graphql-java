package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.Internal;
import graphql.language.Field;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * To prove we can write other execution strategies this one does a breadth first async approach
 * running all fields async first, waiting for the results
 */
@SuppressWarnings("Duplicates")
@Internal
public class BreadthFirstTestStrategy extends ExecutionStrategy {


    public BreadthFirstTestStrategy() {
        super(new SimpleDataFetcherExceptionHandler());
    }

    public static <T> CompletableFuture<List<T>> allOf(final Collection<CompletableFuture<T>> futures) {
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);

        return CompletableFuture.allOf(cfs)
                                .thenApply(vd -> futures.stream()
                                                        .map(CompletableFuture::join)
                                                        .collect(Collectors.toList())
                                );
    }

    @Override
    public Mono<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException {
        return fetchFields(executionContext, parameters)
                .transform(v -> completeFields(executionContext, parameters, v));
    }

    private Mono<Map<String, Object>> fetchFields(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        Map<String, List<Field>> fields = parameters.getFields();

        return Flux.fromIterable(fields.keySet())
                   .flatMap(fieldName -> {
                       ExecutionStrategyParameters newParameters = newParameters(parameters, fields, fieldName);

                       //TODO does not handle null well
                       return Mono.zip(Mono.just(fieldName),
                                       fetchField(executionContext, newParameters));
                   })
                   .collectMap(Tuple2::getT1, Tuple2::getT2);
    }

    private Mono<ExecutionResult> completeFields(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Mono<Map<String, Object>> fetchedValues) {
        Map<String, List<Field>> fields = parameters.getFields();

        return fetchedValues
                .flatMapIterable(Map::entrySet)
                // concat map for breadth-first
                .concatMap(e -> {
                    String fieldName = e.getKey();
                    ExecutionStrategyParameters newParameters = newParameters(parameters, fields, fieldName);
                    Object fetchedValue = e.getValue();

                    return Mono.zip(Mono.just(fieldName),
                                    completeField(executionContext, newParameters, fetchedValue).flatMap(f -> f.getFieldValue()));
                })
                .onErrorResume(NonNullableFieldWasNullException.class, this::assertNonNullFieldPrecondition)
                .collectMap(Tuple2::getT1, t -> t.getT2().getData())
                .map(results -> new ExecutionResultImpl(results, executionContext.getErrors()));
    }

    private ExecutionStrategyParameters newParameters(ExecutionStrategyParameters parameters, Map<String, List<Field>> fields, String fieldName) {
        List<Field> currentField = fields.get(fieldName);
        ExecutionPath fieldPath = parameters.getPath().segment(fieldName);
        return parameters
                .transform(builder -> builder.field(currentField).path(fieldPath));
    }
}
