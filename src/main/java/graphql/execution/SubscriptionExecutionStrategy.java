package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.language.Field;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertTrue;
import static java.util.Collections.singletonMap;

/**
 * An execution strategy that implements graphql subscriptions by using reactive-streams
 * as the output result of the subscription query.
 *
 * Afterwards each object delivered on that stream will be mapped via running the original selection set over that object and hence producing an ExecutionResult
 * just like a normal graphql query.
 *
 * See https://github.com/facebook/graphql/blob/master/spec/Section%206%20--%20Execution.md
 * See http://www.reactive-streams.org/
 */
public class SubscriptionExecutionStrategy extends ExecutionStrategy {

    public SubscriptionExecutionStrategy() {
        super();
    }

    public SubscriptionExecutionStrategy(DataFetcherExceptionHandler dataFetcherExceptionHandler) {
        super(dataFetcherExceptionHandler);
    }

    @Override
    public Mono<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException {

        Mono<Publisher<Object>> sourceEventStream = createSourceEventStream(executionContext, parameters);

        //
        // when the upstream source event stream completes, subscribe to it and wire in our adapter
        return sourceEventStream
                .<ExecutionResult>map(
                        p -> new ExecutionResultImpl(
                                Flux.from(p)
                                    .concatMap(d -> executeSubscriptionEvent(executionContext, parameters, d)),
                                executionContext.getErrors()))
                .switchIfEmpty(Mono.fromCallable(() -> new ExecutionResultImpl(null, executionContext.getErrors())));
    }

    /*
        https://github.com/facebook/graphql/blob/master/spec/Section%206%20--%20Execution.md

        CreateSourceEventStream(subscription, schema, variableValues, initialValue):

            Let {subscriptionType} be the root Subscription type in {schema}.
            Assert: {subscriptionType} is an Object type.
            Let {selectionSet} be the top level Selection Set in {subscription}.
            Let {rootField} be the first top level field in {selectionSet}.
            Let {argumentValues} be the result of {CoerceArgumentValues(subscriptionType, rootField, variableValues)}.
            Let {fieldStream} be the result of running {ResolveFieldEventStream(subscriptionType, initialValue, rootField, argumentValues)}.
            Return {fieldStream}.
     */

    private Mono<Publisher<Object>> createSourceEventStream(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        ExecutionStrategyParameters newParameters = firstFieldOfSubscriptionSelection(parameters);

        Mono<Object> fieldFetched = fetchField(executionContext, newParameters);
        return fieldFetched.map(publisher -> {
            if (publisher != null) {
                assertTrue(publisher instanceof Publisher,
                           "You data fetcher must return a Publisher of events when using graphql subscriptions");
            }
            //noinspection unchecked
            return (Publisher<Object>) publisher;
        });
    }

    /*
        ExecuteSubscriptionEvent(subscription, schema, variableValues, initialValue):

        Let {subscriptionType} be the root Subscription type in {schema}.
        Assert: {subscriptionType} is an Object type.
        Let {selectionSet} be the top level Selection Set in {subscription}.
        Let {data} be the result of running {ExecuteSelectionSet(selectionSet, subscriptionType, initialValue, variableValues)} normally (allowing parallelization).
        Let {errors} be any field errors produced while executing the selection set.
        Return an unordered map containing {data} and {errors}.

        Note: The {ExecuteSubscriptionEvent()} algorithm is intentionally similar to {ExecuteQuery()} since this is how each event result is produced.
     */

    private Mono<ExecutionResult> executeSubscriptionEvent(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Object eventPayload) {
        ExecutionContext newExecutionContext = executionContext.transform(builder -> builder.root(eventPayload));

        ExecutionStrategyParameters newParameters = firstFieldOfSubscriptionSelection(parameters);

        return completeField(newExecutionContext, newParameters, eventPayload)
                .flatMap(FieldValueInfo::getFieldValue)
                .map(executionResult -> wrapWithRootFieldName(newParameters, executionResult));
    }

    private ExecutionResult wrapWithRootFieldName(ExecutionStrategyParameters parameters, ExecutionResult executionResult) {
        String rootFieldName = getRootFieldName(parameters);
        return new ExecutionResultImpl(
                singletonMap(rootFieldName, executionResult.getData()),
                executionResult.getErrors()
        );
    }

    private String getRootFieldName(ExecutionStrategyParameters parameters) {
        Field rootField = parameters.getField().get(0);
        return rootField.getAlias() != null ? rootField.getAlias() : rootField.getName();
    }

    private ExecutionStrategyParameters firstFieldOfSubscriptionSelection(ExecutionStrategyParameters parameters) {
        Map<String, List<Field>> fields = parameters.getFields();
        List<String> fieldNames = new ArrayList<>(fields.keySet());

        List<Field> firstField = fields.get(fieldNames.get(0));

        ExecutionPath fieldPath = parameters.getPath().segment(mkNameForPath(firstField));
        return parameters.transform(builder -> builder.field(firstField).path(fieldPath));
    }

}
