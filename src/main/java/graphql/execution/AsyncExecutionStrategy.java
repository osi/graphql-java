package graphql.execution;

import graphql.ExecutionResult;
import graphql.execution.defer.DeferSupport;
import graphql.execution.defer.DeferredCall;
import graphql.execution.defer.DeferredErrorSupport;
import graphql.execution.instrumentation.DeferredFieldInstrumentationContext;
import graphql.execution.instrumentation.ExecutionStrategyInstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationDeferredFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import graphql.language.Field;
import graphql.schema.GraphQLFieldDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The standard graphql execution strategy that runs fields asynchronously non-blocking.
 */
public class AsyncExecutionStrategy extends AbstractAsyncExecutionStrategy {

    /**
     * The standard graphql execution strategy that runs fields asynchronously
     */
    public AsyncExecutionStrategy() {
        super(new SimpleDataFetcherExceptionHandler());
    }

    /**
     * Creates a execution strategy that uses the provided exception handler
     *
     * @param exceptionHandler the exception handler to use
     */
    public AsyncExecutionStrategy(DataFetcherExceptionHandler exceptionHandler) {
        super(exceptionHandler);
    }

    @Override
    public Mono<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        ExecutionStrategyInstrumentationContext executionStrategyCtx =
                executionContext.getInstrumentation()
                                .beginExecutionStrategy(
                                        new InstrumentationExecutionStrategyParameters(executionContext, parameters));

        return Flux.fromIterable(parameters.getFields().entrySet())
                   .flatMap(e -> {
                                String fieldName = e.getKey();
                                List<Field> currentField = e.getValue();
                                ExecutionPath fieldPath = parameters.getPath().segment(mkNameForPath(currentField));
                                ExecutionStrategyParameters newParameters = parameters
                                        .transform(builder -> builder.field(currentField).path(fieldPath).parent(parameters));

                                // TODO
                                if (isDeferred(executionContext, newParameters, currentField)) {
                                    executionStrategyCtx.onDeferredField(currentField);
                                    return Mono.empty();
                                }

                                return Mono.zip(Mono.just(fieldName),
                                                resolveFieldWithInfo(executionContext, newParameters));
                            }
                   )
                   .collectList()
                   // TODO look into this - i think for async
                   .doOnNext(l -> executionStrategyCtx.onFieldValuesInfo(l.stream()
                                                                          .map(Tuple2::getT2)
                                                                          .collect(Collectors.toList())))
                   .flatMapMany(l -> Flux.fromIterable(l)
                                         .flatMap(t -> Mono.zip(Mono.just(t.getT1()),
                                                                t.getT2().getFieldValue())))
                   .as(f -> handleResults(f, executionContext, executionStrategyCtx));
    }

    private boolean isDeferred(ExecutionContext executionContext, ExecutionStrategyParameters parameters, List<Field> currentField) {
        DeferSupport deferSupport = executionContext.getDeferSupport();
        // with a deferred field we are really resetting where we execute from, that is from this current field onwards
        // this is a break in the parent -> child chain - its a new start effectively
        if (!deferSupport.checkForDeferDirective(currentField)) {
            return false;
        }

        DeferredErrorSupport errorSupport = new DeferredErrorSupport();

        // with a deferred field we are really resetting where we execute from, that is from this current field onwards
        Map<String, List<Field>> fields = new HashMap<>();
        fields.put(currentField.get(0).getName(), currentField);

        ExecutionStrategyParameters callParameters =
                parameters.transform(builder -> builder.deferredErrorSupport(errorSupport)
                                                       .field(currentField)
                                                       .fields(fields)
                                                       .parent(null) // this is a break in the parent -> child chain - its a new start effectively
                                                       .listSize(0)
                                                       .currentListIndex(0)
                );

        DeferredCall call = new DeferredCall(deferredExecutionResult(executionContext, callParameters), errorSupport);
        deferSupport.enqueue(call);
        return true;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private Mono<ExecutionResult> deferredExecutionResult(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        return Mono.defer(() -> {
            GraphQLFieldDefinition fieldDef = getFieldDef(executionContext, parameters, parameters.getField().get(0));

            DeferredFieldInstrumentationContext fieldCtx =
                    executionContext.getInstrumentation()
                                    .beginDeferredField(
                                            new InstrumentationDeferredFieldParameters(executionContext,
                                                                                       parameters,
                                                                                       fieldDef,
                                                                                       fieldTypeInfo(parameters,
                                                                                                     fieldDef))
                                    );

            return resolveFieldWithInfo(executionContext, parameters)
                    .doOnSuccess(fieldCtx::onFieldValueInfo)
                    .flatMap(FieldValueInfo::getFieldValue)
                    .transform(fieldCtx::instrument);
        });
    }
}
