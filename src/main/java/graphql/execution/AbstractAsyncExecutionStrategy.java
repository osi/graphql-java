package graphql.execution;


import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.execution.instrumentation.InstrumentationContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public abstract class AbstractAsyncExecutionStrategy extends ExecutionStrategy {

    public AbstractAsyncExecutionStrategy(DataFetcherExceptionHandler dataFetcherExceptionHandler) {
        super(dataFetcherExceptionHandler);
    }

    protected Mono<ExecutionResult> handleResults(Flux<Tuple2<String, ExecutionResult>> executionResultsByField,
                                                  ExecutionContext executionContext,
                                                  InstrumentationContext<ExecutionResult> executionStrategyCtx) {
        return executionResultsByField
                .collectMap(Tuple2::getT1, t -> t.getT2().getData())
                .<ExecutionResult>map(resolvedValuesByField -> new ExecutionResultImpl(resolvedValuesByField,
                                                                                       executionContext.getErrors()))
                .transform(m -> handleNonNullException(m, executionContext))
                .transform(executionStrategyCtx::instrument);
    }
}
