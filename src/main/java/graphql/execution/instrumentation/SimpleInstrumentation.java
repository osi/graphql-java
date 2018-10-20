package graphql.execution.instrumentation;

import graphql.ExecutionResult;
import graphql.PublicApi;
import graphql.execution.ExecutionContext;
import graphql.execution.instrumentation.parameters.InstrumentationDeferredFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldCompleteParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldParameters;
import graphql.execution.instrumentation.parameters.InstrumentationValidationParameters;
import graphql.language.Document;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLSchema;
import graphql.validation.ValidationError;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * An implementation of {@link graphql.execution.instrumentation.Instrumentation} that does nothing.  It can be used
 * as a base for derived classes where you only implement the methods you want to
 */
@PublicApi
public class SimpleInstrumentation implements Instrumentation {

    /**
     * A singleton instance of a {@link graphql.execution.instrumentation.Instrumentation} that does nothing
     */
    public static final SimpleInstrumentation INSTANCE = new SimpleInstrumentation();

    public SimpleInstrumentation() {
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginExecution(InstrumentationExecutionParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public InstrumentationContext<Document> beginParse(InstrumentationExecutionParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public InstrumentationContext<List<ValidationError>> beginValidation(InstrumentationValidationParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public ExecutionStrategyInstrumentationContext beginExecutionStrategy(InstrumentationExecutionStrategyParameters parameters) {
        return new ExecutionStrategyInstrumentationContext() {
            @Override
            public Mono<ExecutionResult> onDispatched(Mono<ExecutionResult> result) {
                return result;
            }

            @Override
            public void onCompleted(ExecutionResult result, Throwable t) {

            }
        };
    }

    @Override
    public DeferredFieldInstrumentationContext beginDeferredField(InstrumentationDeferredFieldParameters parameters) {
        return new DeferredFieldInstrumentationContext() {
            @Override
            public Mono<ExecutionResult> onDispatched(Mono<ExecutionResult> result) {
                return result;
            }

            @Override
            public void onCompleted(ExecutionResult result, Throwable t) {

            }
        };
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginExecuteOperation(InstrumentationExecuteOperationParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginField(InstrumentationFieldParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public InstrumentationContext<Object> beginFieldFetch(InstrumentationFieldFetchParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }


    @Override
    public InstrumentationContext<ExecutionResult> beginFieldComplete(InstrumentationFieldCompleteParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public InstrumentationContext<ExecutionResult> beginFieldListComplete(InstrumentationFieldCompleteParameters parameters) {
        return new SimpleInstrumentationContext<>();
    }

    @Override
    public GraphQLSchema instrumentSchema(GraphQLSchema schema, InstrumentationExecutionParameters parameters) {
        return schema;
    }

    @Override
    public ExecutionContext instrumentExecutionContext(ExecutionContext executionContext, InstrumentationExecutionParameters parameters) {
        return executionContext;
    }

    @Override
    public DataFetcher<?> instrumentDataFetcher(DataFetcher<?> dataFetcher, InstrumentationFieldFetchParameters parameters) {
        return dataFetcher;
    }

    @Override
    public Mono<ExecutionResult> instrumentExecutionResult(ExecutionResult executionResult, InstrumentationExecutionParameters parameters) {
        return Mono.just(executionResult);
    }

}
