package graphql.execution;

import graphql.ExecutionResult;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import graphql.language.Field;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Async non-blocking execution, but serial: only one field at the the time will be resolved.
 * See {@link AsyncExecutionStrategy} for a non serial (parallel) execution of every field.
 */
public class AsyncSerialExecutionStrategy extends AbstractAsyncExecutionStrategy {

    public AsyncSerialExecutionStrategy() {
        super(new SimpleDataFetcherExceptionHandler());
    }

    public AsyncSerialExecutionStrategy(DataFetcherExceptionHandler exceptionHandler) {
        super(exceptionHandler);
    }

    @Override
    public Mono<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {

        InstrumentationContext<ExecutionResult> executionStrategyCtx =
                executionContext.getInstrumentation()
                                .beginExecutionStrategy(
                                        new InstrumentationExecutionStrategyParameters(executionContext, parameters));

        return Flux.fromIterable(parameters.getFields().entrySet())
                   .concatMap(e -> {
                                  String fieldName = e.getKey();
                                  List<Field> currentField = e.getValue();
                                  ExecutionPath fieldPath = parameters.getPath().segment(
                                          mkNameForPath(currentField));
                                  ExecutionStrategyParameters newParameters = parameters
                                          .transform(builder -> builder.field(
                                                  currentField).path(fieldPath).parent(
                                                  parameters));

                                  return Mono.zip(Mono.just(fieldName),
                                                  resolveField(executionContext,
                                                               newParameters));
                              }
                   )
                   .as(f -> handleResults(f, executionContext, executionStrategyCtx));
    }

}
