package graphql.execution;


import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.defer.DeferSupport;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.NodeUtil;
import graphql.language.OperationDefinition;
import graphql.language.VariableDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static graphql.Assert.assertShouldNeverHappen;
import static graphql.execution.ExecutionContextBuilder.newExecutionContextBuilder;
import static graphql.execution.ExecutionStrategyParameters.newParameters;
import static graphql.execution.ExecutionTypeInfo.newTypeInfo;
import static graphql.language.OperationDefinition.Operation.MUTATION;
import static graphql.language.OperationDefinition.Operation.QUERY;
import static graphql.language.OperationDefinition.Operation.SUBSCRIPTION;

@Internal
public class Execution {
    private static final Logger log = LoggerFactory.getLogger(Execution.class);

    private final FieldCollector fieldCollector = new FieldCollector();
    private final ExecutionStrategy queryStrategy;
    private final ExecutionStrategy mutationStrategy;
    private final ExecutionStrategy subscriptionStrategy;
    private final Instrumentation instrumentation;

    public Execution(ExecutionStrategy queryStrategy, ExecutionStrategy mutationStrategy, ExecutionStrategy subscriptionStrategy, Instrumentation instrumentation) {
        this.queryStrategy = queryStrategy != null ? queryStrategy : new AsyncExecutionStrategy();
        this.mutationStrategy = mutationStrategy != null ? mutationStrategy : new AsyncSerialExecutionStrategy();
        this.subscriptionStrategy = subscriptionStrategy != null ? subscriptionStrategy : new AsyncExecutionStrategy();
        this.instrumentation = instrumentation;
    }

    public Mono<ExecutionResult> execute(Document document, GraphQLSchema graphQLSchema, ExecutionId executionId,
                                         ExecutionInput executionInput) {
        return Mono.defer(() -> {
            NodeUtil.GetOperationResult getOperationResult = NodeUtil.getOperation(document,
                                                                                   executionInput.getOperationName());
            Map<String, FragmentDefinition> fragmentsByName = getOperationResult.fragmentsByName;
            OperationDefinition operationDefinition = getOperationResult.operationDefinition;

            ValuesResolver valuesResolver = new ValuesResolver();
            Map<String, Object> inputVariables = executionInput.getVariables();
            List<VariableDefinition> variableDefinitions = operationDefinition.getVariableDefinitions();

            return Mono.fromCallable(
                    () -> valuesResolver.coerceArgumentValues(graphQLSchema, variableDefinitions, inputVariables))
                       .zipWith(GraphQL.instrumentationState(),
                                (coercedVariables, instrumentationState) ->
                                        newExecutionContextBuilder()
                                                .instrumentation(instrumentation)
                                                .instrumentationState(instrumentationState)
                                                .executionId(executionId)
                                                .graphQLSchema(graphQLSchema)
                                                .queryStrategy(queryStrategy)
                                                .mutationStrategy(mutationStrategy)
                                                .subscriptionStrategy(subscriptionStrategy)
                                                .context(executionInput.getContext())
                                                .root(executionInput.getRoot())
                                                .fragmentsByName(fragmentsByName)
                                                .variables(coercedVariables)
                                                .document(document)
                                                .operationDefinition(operationDefinition)
                                                .build())
                       .map(executionContext -> instrumentation.instrumentExecutionContext(
                               executionContext,
                               new InstrumentationExecutionParameters(
                                       executionInput,
                                       graphQLSchema,
                                       executionContext.getInstrumentationState()
                               )))
                       .flatMap(executionContext -> executeOperation(executionContext,
                                                                     executionInput.getRoot(),
                                                                     executionContext.getOperationDefinition()))
                       // The abort needs to bubble all the way to the top
                       .onErrorResume(t -> t instanceof GraphQLError && !(t instanceof AbortExecutionException),
                                      e -> Mono.just(new ExecutionResultImpl((GraphQLError) e)))
                    ;
        });
    }


    private Mono<ExecutionResult> executeOperation(ExecutionContext executionContext, Object root, OperationDefinition operationDefinition) {
        // TODO context-ify
        InstrumentationExecuteOperationParameters instrumentationParams = new InstrumentationExecuteOperationParameters(
                executionContext);
        InstrumentationContext<ExecutionResult> executeOperationCtx = instrumentation.beginExecuteOperation(
                instrumentationParams);

        return executeOperation2(executionContext, root, operationDefinition)
                .onErrorResume(NonNullableFieldWasNullException.class,
                               t -> {
                                   // this means it was non null types all the way from an offending non null type
                                   // up to the root object type and there was a a null value some where.
                                   //
                                   // The spec says we should return null for the data in this case
                                   //
                                   // http://facebook.github.io/graphql/#sec-Errors-and-Non-Nullability
                                   //
                                   return Mono.just(new ExecutionResultImpl(null, executionContext.getErrors()));
                               }
                )
                .transform(executeOperationCtx::instrument)
                .transform(m -> deferSupport(executionContext, m));
    }

    private Mono<ExecutionResult> executeOperation2(ExecutionContext executionContext, Object root, OperationDefinition operationDefinition) {
        OperationDefinition.Operation operation = operationDefinition.getOperation();
        GraphQLObjectType operationRootType = getOperationRootType(executionContext.getGraphQLSchema(),
                                                                   operationDefinition);

        FieldCollectorParameters collectorParameters =
                FieldCollectorParameters.newParameters()
                                        .schema(executionContext.getGraphQLSchema())
                                        .objectType(operationRootType)
                                        .fragments(executionContext.getFragmentsByName())
                                        .variables(executionContext.getVariables())
                                        .build();

        Map<String, List<Field>> fields = fieldCollector.collectFields(collectorParameters,
                                                                       operationDefinition.getSelectionSet());

        ExecutionPath path = ExecutionPath.rootPath();
        ExecutionTypeInfo typeInfo = newTypeInfo().type(operationRootType).path(path).build();
        NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext, typeInfo);

        ExecutionStrategyParameters parameters = newParameters()
                .typeInfo(typeInfo)
                .source(root)
                .fields(fields)
                .nonNullFieldValidator(nonNullableFieldValidator)
                .path(path)
                .build();

        ExecutionStrategy executionStrategy;
        if (operation == OperationDefinition.Operation.MUTATION) {
            executionStrategy = mutationStrategy;
        } else if (operation == SUBSCRIPTION) {
            executionStrategy = subscriptionStrategy;
        } else {
            executionStrategy = queryStrategy;
        }
        log.debug("Executing '{}' query operation: '{}' using '{}' execution strategy",
                  executionContext.getExecutionId(), operation, executionStrategy.getClass().getName());
        return executionStrategy.execute(executionContext, parameters);
    }

    /*
     * Adds the deferred publisher if its needed at the end of the query.  This is also a good time for the deferred code to start running
     */
    private Mono<ExecutionResult> deferSupport(ExecutionContext executionContext, Mono<ExecutionResult> result) {
        return result.map(er -> {
            DeferSupport deferSupport = executionContext.getDeferSupport();
            if (deferSupport.isDeferDetected()) {
                // we start the rest of the query now to maximize throughput.  We have the initial important results
                // and now we can start the rest of the calls as early as possible (even before some one subscribes)
                Publisher<ExecutionResult> publisher = deferSupport.startDeferredCalls();
                return ExecutionResultImpl.newExecutionResult().from((ExecutionResultImpl) er)
                                          .addExtension(GraphQL.DEFERRED_RESULTS, publisher)
                                          .build();
            }
            return er;
        });

    }

    private GraphQLObjectType getOperationRootType(GraphQLSchema graphQLSchema, OperationDefinition operationDefinition) {
        OperationDefinition.Operation operation = operationDefinition.getOperation();
        if (operation == MUTATION) {
            GraphQLObjectType mutationType = graphQLSchema.getMutationType();
            if (mutationType == null) {
                throw new MissingRootTypeException("Schema is not configured for mutations.",
                                                   operationDefinition.getSourceLocation());
            }
            return mutationType;
        } else if (operation == QUERY) {
            GraphQLObjectType queryType = graphQLSchema.getQueryType();
            if (queryType == null) {
                throw new MissingRootTypeException("Schema does not define the required query root type.",
                                                   operationDefinition.getSourceLocation());
            }
            return queryType;
        } else if (operation == SUBSCRIPTION) {
            GraphQLObjectType subscriptionType = graphQLSchema.getSubscriptionType();
            if (subscriptionType == null) {
                throw new MissingRootTypeException("Schema is not configured for subscriptions.",
                                                   operationDefinition.getSourceLocation());
            }
            return subscriptionType;
        } else {
            return assertShouldNeverHappen(
                    "Unhandled case.  An extra operation enum has been added without code support");
        }
    }
}
