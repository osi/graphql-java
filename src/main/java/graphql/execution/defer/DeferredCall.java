package graphql.execution.defer;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQLError;
import graphql.Internal;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * This represents a deferred call (aka @defer) to get an execution result sometime after
 * the initial query has returned
 */
@Internal
public class DeferredCall {
    private final Mono<ExecutionResult> call;
    private final DeferredErrorSupport errorSupport;

    public DeferredCall(Mono<ExecutionResult> call, DeferredErrorSupport deferredErrorSupport) {
        this.call = call;
        this.errorSupport = deferredErrorSupport;
    }

    Mono<ExecutionResult> invoke() {
        return call.map(this::addErrorsEncountered);
    }

    private ExecutionResult addErrorsEncountered(ExecutionResult executionResult) {
        List<GraphQLError> errorsEncountered = errorSupport.getErrors();
        if (errorsEncountered.isEmpty()) {
            return executionResult;
        }
        ExecutionResultImpl sourceResult = (ExecutionResultImpl) executionResult;
        ExecutionResultImpl.Builder builder = ExecutionResultImpl.newExecutionResult().from(sourceResult);
        builder.addErrors(errorsEncountered);
        return builder.build();
    }

}
