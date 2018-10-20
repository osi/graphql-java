package graphql;

import graphql.execution.AbortExecutionException;
import graphql.execution.AsyncExecutionStrategy;
import graphql.execution.AsyncSerialExecutionStrategy;
import graphql.execution.Execution;
import graphql.execution.ExecutionId;
import graphql.execution.ExecutionIdProvider;
import graphql.execution.ExecutionStrategy;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationValidationParameters;
import graphql.execution.preparsed.NoOpPreparsedDocumentProvider;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.execution.preparsed.PreparsedDocumentProvider;
import graphql.language.Document;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.validation.ValidationError;
import graphql.validation.Validator;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static graphql.Assert.assertNotNull;
import static graphql.InvalidSyntaxError.toInvalidSyntaxError;

/**
 * This class is where all graphql-java query execution begins.  It combines the objects that are needed
 * to make a successful graphql query, with the most important being the {@link graphql.schema.GraphQLSchema schema}
 * and the {@link graphql.execution.ExecutionStrategy execution strategy}
 *
 * Building this object is very cheap and can be done on each execution if necessary.  Building the schema is often not
 * as cheap, especially if its parsed from graphql IDL schema format via {@link graphql.schema.idl.SchemaParser}.
 *
 * The data for a query is returned via {@link ExecutionResult#getData()} and any errors encountered as placed in
 * {@link ExecutionResult#getErrors()}.
 *
 * <h2>Runtime Exceptions</h2>
 *
 * Runtime exceptions can be thrown by the graphql engine if certain situations are encountered.  These are not errors
 * in execution but rather totally unacceptable conditions in which to execute a graphql query.
 * <ul>
 * <li>{@link graphql.schema.CoercingSerializeException} - is thrown when a value cannot be serialised by a Scalar type, for example
 * a String value being coerced as an Int.
 * </li>
 *
 * <li>{@link graphql.execution.UnresolvedTypeException} - is thrown if a {@link graphql.schema.TypeResolver} fails to provide a concrete
 * object type given a interface or union type.
 * </li>
 *
 * <li>{@link graphql.schema.validation.InvalidSchemaException} - is thrown if the schema is not valid when built via
 * {@link graphql.schema.GraphQLSchema.Builder#build()}
 * </li>
 *
 * <li>{@link graphql.GraphQLException} - is thrown as a general purpose runtime exception, for example if the code cant
 * access a named field when examining a POJO.
 * </li>
 *
 * <li>{@link graphql.AssertException} - is thrown as a low level code assertion exception for truly unexpected code conditions
 * </li>
 *
 * </ul>
 */
@PublicApi
public class GraphQL {

    /**
     * When @defer directives are used, this is the extension key name used to contain the {@link org.reactivestreams.Publisher}
     * of deferred results
     */
    public static final String DEFERRED_RESULTS = "deferredResults";

    private static final Logger log = LoggerFactory.getLogger(GraphQL.class);

    private static final ExecutionIdProvider DEFAULT_EXECUTION_ID_PROVIDER = (query, operationName, context) -> ExecutionId.generate();

    private final GraphQLSchema graphQLSchema;
    private final ExecutionStrategy queryStrategy;
    private final ExecutionStrategy mutationStrategy;
    private final ExecutionStrategy subscriptionStrategy;
    private final ExecutionIdProvider idProvider;
    private final Instrumentation instrumentation;
    private final PreparsedDocumentProvider preparsedDocumentProvider;
    private final Execution execution;


    /**
     * A GraphQL object ready to execute queries
     *
     * @param graphQLSchema the schema to use
     *
     * @deprecated use the {@link #newGraphQL(GraphQLSchema)} builder instead.  This will be removed in a future version.
     */
    @Internal
    public GraphQL(GraphQLSchema graphQLSchema) {
        //noinspection deprecation
        this(graphQLSchema, null, null);
    }

    /**
     * A GraphQL object ready to execute queries
     *
     * @param graphQLSchema the schema to use
     * @param queryStrategy the query execution strategy to use
     *
     * @deprecated use the {@link #newGraphQL(GraphQLSchema)} builder instead.  This will be removed in a future version.
     */
    @Internal
    public GraphQL(GraphQLSchema graphQLSchema, ExecutionStrategy queryStrategy) {
        //noinspection deprecation
        this(graphQLSchema, queryStrategy, null);
    }

    /**
     * A GraphQL object ready to execute queries
     *
     * @param graphQLSchema    the schema to use
     * @param queryStrategy    the query execution strategy to use
     * @param mutationStrategy the mutation execution strategy to use
     *
     * @deprecated use the {@link #newGraphQL(GraphQLSchema)} builder instead.  This will be removed in a future version.
     */
    @Internal
    public GraphQL(GraphQLSchema graphQLSchema, ExecutionStrategy queryStrategy, ExecutionStrategy mutationStrategy) {
        this(graphQLSchema, queryStrategy, mutationStrategy, null, DEFAULT_EXECUTION_ID_PROVIDER,
             SimpleInstrumentation.INSTANCE, NoOpPreparsedDocumentProvider.INSTANCE);
    }

    /**
     * A GraphQL object ready to execute queries
     *
     * @param graphQLSchema        the schema to use
     * @param queryStrategy        the query execution strategy to use
     * @param mutationStrategy     the mutation execution strategy to use
     * @param subscriptionStrategy the subscription execution strategy to use
     *
     * @deprecated use the {@link #newGraphQL(GraphQLSchema)} builder instead.  This will be removed in a future version.
     */
    @Internal
    public GraphQL(GraphQLSchema graphQLSchema, ExecutionStrategy queryStrategy, ExecutionStrategy mutationStrategy, ExecutionStrategy subscriptionStrategy) {
        this(graphQLSchema, queryStrategy, mutationStrategy, subscriptionStrategy, DEFAULT_EXECUTION_ID_PROVIDER,
             SimpleInstrumentation.INSTANCE, NoOpPreparsedDocumentProvider.INSTANCE);
    }

    private GraphQL(GraphQLSchema graphQLSchema, ExecutionStrategy queryStrategy, ExecutionStrategy mutationStrategy, ExecutionStrategy subscriptionStrategy, ExecutionIdProvider idProvider, Instrumentation instrumentation, PreparsedDocumentProvider preparsedDocumentProvider) {
        this.graphQLSchema = assertNotNull(graphQLSchema, "graphQLSchema must be non null");
        this.queryStrategy = queryStrategy != null ? queryStrategy : new AsyncExecutionStrategy();
        this.mutationStrategy = mutationStrategy != null ? mutationStrategy : new AsyncSerialExecutionStrategy();
        this.subscriptionStrategy = subscriptionStrategy != null ? subscriptionStrategy : new SubscriptionExecutionStrategy();
        this.idProvider = assertNotNull(idProvider, "idProvider must be non null");
        this.instrumentation = instrumentation;
        this.preparsedDocumentProvider = assertNotNull(preparsedDocumentProvider,
                                                       "preparsedDocumentProvider must be non null");
        execution = new Execution(queryStrategy, mutationStrategy, subscriptionStrategy, instrumentation);
    }

    /**
     * Helps you build a GraphQL object ready to execute queries
     *
     * @param graphQLSchema the schema to use
     *
     * @return a builder of GraphQL objects
     */
    public static Builder newGraphQL(GraphQLSchema graphQLSchema) {
        return new Builder(graphQLSchema);
    }

    private static <T> T nvl(T obj, T elseObj) {
        return obj == null ? elseObj : obj;
    }

    private static InstrumentationState instrumentationState(Context c) {
        return c.getOrDefault(InstrumentationState.class, null);
    }

    /**
     * This helps you transform the current GraphQL object into another one by starting a builder with all
     * the current values and allows you to transform it how you want.
     *
     * @param builderConsumer the consumer code that will be given a builder to transform
     *
     * @return a new GraphQL object based on calling build on that builder
     */
    public GraphQL transform(Consumer<GraphQL.Builder> builderConsumer) {
        Builder builder = new Builder(graphQLSchema);
        builder
                .queryExecutionStrategy(nvl(queryStrategy, builder.queryExecutionStrategy))
                .mutationExecutionStrategy(nvl(mutationStrategy, builder.mutationExecutionStrategy))
                .subscriptionExecutionStrategy(nvl(subscriptionStrategy, builder.subscriptionExecutionStrategy))
                .executionIdProvider(nvl(idProvider, builder.idProvider))
                .instrumentation(nvl(instrumentation, builder.instrumentation))
                .preparsedDocumentProvider(nvl(preparsedDocumentProvider, builder.preparsedDocumentProvider));

        builderConsumer.accept(builder);

        return builder.build();
    }

    /**
     * Executes the specified graphql query/mutation/subscription
     *
     * @param query the query/mutation/subscription
     *
     * @return an {@link ExecutionResult} which can include errors
     */
    public ExecutionResult execute(String query) {
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                                                      .query(query)
                                                      .build();
        return execute(executionInput);
    }

    /**
     * Info: This sets context = root to be backwards compatible.
     *
     * @param query   the query/mutation/subscription
     * @param context custom object provided to each {@link graphql.schema.DataFetcher}
     *
     * @return an {@link ExecutionResult} which can include errors
     *
     * @deprecated Use {@link #execute(ExecutionInput)}
     */
    @Deprecated
    public ExecutionResult execute(String query, Object context) {
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                                                      .query(query)
                                                      .context(context)
                                                      .root(context) // This we are doing do be backwards compatible
                                                      .build();
        return execute(executionInput);
    }

    /**
     * Info: This sets context = root to be backwards compatible.
     *
     * @param query         the query/mutation/subscription
     * @param operationName the name of the operation to execute
     * @param context       custom object provided to each {@link graphql.schema.DataFetcher}
     *
     * @return an {@link ExecutionResult} which can include errors
     *
     * @deprecated Use {@link #execute(ExecutionInput)}
     */
    @Deprecated
    public ExecutionResult execute(String query, String operationName, Object context) {
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                                                      .query(query)
                                                      .operationName(operationName)
                                                      .context(context)
                                                      .root(context) // This we are doing do be backwards compatible
                                                      .build();
        return execute(executionInput);
    }

    /**
     * Info: This sets context = root to be backwards compatible.
     *
     * @param query     the query/mutation/subscription
     * @param context   custom object provided to each {@link graphql.schema.DataFetcher}
     * @param variables variable values uses as argument
     *
     * @return an {@link ExecutionResult} which can include errors
     *
     * @deprecated Use {@link #execute(ExecutionInput)}
     */
    @Deprecated
    public ExecutionResult execute(String query, Object context, Map<String, Object> variables) {
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                                                      .query(query)
                                                      .context(context)
                                                      .root(context) // This we are doing do be backwards compatible
                                                      .variables(variables)
                                                      .build();
        return execute(executionInput);
    }

    /**
     * Info: This sets context = root to be backwards compatible.
     *
     * @param query         the query/mutation/subscription
     * @param operationName name of the operation to execute
     * @param context       custom object provided to each {@link graphql.schema.DataFetcher}
     * @param variables     variable values uses as argument
     *
     * @return an {@link ExecutionResult} which can include errors
     *
     * @deprecated Use {@link #execute(ExecutionInput)}
     */
    @Deprecated
    public ExecutionResult execute(String query, String operationName, Object context, Map<String, Object> variables) {
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                                                      .query(query)
                                                      .operationName(operationName)
                                                      .context(context)
                                                      .root(context) // This we are doing do be backwards compatible
                                                      .variables(variables)
                                                      .build();
        return execute(executionInput);
    }

    /**
     * Executes the graphql query using the provided input object builder
     *
     * @param executionInputBuilder {@link ExecutionInput.Builder}
     *
     * @return an {@link ExecutionResult} which can include errors
     */
    public ExecutionResult execute(ExecutionInput.Builder executionInputBuilder) {
        return execute(executionInputBuilder.build());
    }

    /**
     * Executes the graphql query using calling the builder function and giving it a new builder.
     * <p>
     * This allows a lambda style like :
     * <pre>
     * {@code
     *    ExecutionResult result = graphql.execute(input -> input.query("{hello}").root(startingObj).context(contextObj));
     * }
     * </pre>
     *
     * @param builderFunction a function that is given a {@link ExecutionInput.Builder}
     *
     * @return an {@link ExecutionResult} which can include errors
     */
    public ExecutionResult execute(UnaryOperator<ExecutionInput.Builder> builderFunction) {
        return execute(builderFunction.apply(ExecutionInput.newExecutionInput()).build());
    }

    /**
     * Executes the graphql query using the provided input object
     *
     * @param executionInput {@link ExecutionInput}
     *
     * @return an {@link ExecutionResult} which can include errors
     */
    public ExecutionResult execute(ExecutionInput executionInput) {
        return executeAsync(executionInput).block();
    }

    /**
     * Executes the graphql query using the provided input object builder
     * <p>
     * This will return a promise (aka {@link CompletableFuture}) to provide a {@link ExecutionResult}
     * which is the result of executing the provided query.
     *
     * @param executionInputBuilder {@link ExecutionInput.Builder}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public Mono<ExecutionResult> executeAsync(ExecutionInput.Builder executionInputBuilder) {
        return executeAsync(executionInputBuilder.build());
    }

    /**
     * Executes the graphql query using the provided input object builder
     * <p>
     * This will return a promise (aka {@link CompletableFuture}) to provide a {@link ExecutionResult}
     * which is the result of executing the provided query.
     * <p>
     * This allows a lambda style like :
     * <pre>
     * {@code
     *    ExecutionResult result = graphql.execute(input -> input.query("{hello}").root(startingObj).context(contextObj));
     * }
     * </pre>
     *
     * @param builderFunction a function that is given a {@link ExecutionInput.Builder}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public Mono<ExecutionResult> executeAsync(UnaryOperator<ExecutionInput.Builder> builderFunction) {
        return executeAsync(builderFunction.apply(ExecutionInput.newExecutionInput()).build());
    }

    /**
     * Executes the graphql query using the provided input object
     * <p>
     * This will return a promise (aka {@link CompletableFuture}) to provide a {@link ExecutionResult}
     * which is the result of executing the provided query.
     *
     * @param executionInput {@link ExecutionInput}
     *
     * @return a promise to an {@link ExecutionResult} which can include errors
     */
    public Mono<ExecutionResult> executeAsync(ExecutionInput executionInput) {
        return context(GraphQL::instrumentationState)
                .doOnSubscribe(s -> log.debug("Executing request. operation name: '{}'. query: '{}'. variables '{}'",
                                              executionInput.getOperationName(),
                                              executionInput.getQuery(),
                                              executionInput.getVariables()))
                .flatMap(
                        state -> Mono.just(new InstrumentationExecutionParameters(executionInput, graphQLSchema, state))
                                     .map(p -> instrumentation.instrumentExecutionInput(executionInput, p))
                                     .map(ei -> new InstrumentationExecutionParameters(ei, graphQLSchema, state)))
                .flatMap(p -> Mono.fromCallable(() -> instrumentation.instrumentSchema(graphQLSchema, p))
                                  .flatMap(schema -> parseValidateAndExecute(p.getExecutionInput(), schema))
                                  // TODO i feel i missed a instrumentation step here
                                  .flatMap(r -> instrumentation.instrumentExecutionResult(r, p))
                                  .transform(instrumentation.beginExecution(p)::instrument))
                .subscriberContext(this::withNewInstrumentState)
                .onErrorResume(AbortExecutionException.class, e -> Mono.just(e.toExecutionResult()));
    }

    private Context withNewInstrumentState(Context c) {
        return c.put(InstrumentationState.class, instrumentation.createState());
    }

    private <T, R> Mono<R> context(Function<Context, R> transform) {
        return Mono.subscriberContext().flatMap(c -> Mono.just(transform.apply(c)));
    }


    private Mono<ExecutionResult> parseValidateAndExecute(ExecutionInput executionInput, GraphQLSchema graphQLSchema) {
        return Mono.just(executionInput.getQuery())
                   .map(preparsedDocumentProvider::canonicalize)
                   .map(q -> executionInput.transform(b -> b.query(q)))
                   .flatMap(ei -> preparsedDocumentProvider.get(ei.getQuery(),
                                                                parseAndValidate(ei, graphQLSchema))
                                                           .flatMap(entry -> {
                                                               if (entry.hasErrors()) {
                                                                   return Mono.just(
                                                                           new ExecutionResultImpl(entry.getErrors()));
                                                               }

                                                               return execute(ei,
                                                                              entry.getDocument(),
                                                                              graphQLSchema
                                                               );
                                                           }));
    }

    private Mono<PreparsedDocumentEntry> parseAndValidate(ExecutionInput executionInput, GraphQLSchema graphQLSchema) {
        return Mono.defer(() -> {
            log.debug("Parsing query: '{}'...", executionInput.getQuery());
            return parse(executionInput, graphQLSchema);
        }).flatMap(parseResult -> {
            if (parseResult.isFailure()) {
                log.warn("Query failed to parse : '{}'", executionInput.getQuery());
                return Mono.just(new PreparsedDocumentEntry(toInvalidSyntaxError(parseResult.getException())));
            }
            Document document = parseResult.getDocument();

            log.debug("Validating query: '{}'", executionInput.getQuery());
            return validate(executionInput, document, graphQLSchema)
                    .map(errors -> {
                        if (!errors.isEmpty()) {
                            log.warn("Query failed to validate : '{}'", executionInput.getQuery());
                            return new PreparsedDocumentEntry(errors);
                        }

                        return new PreparsedDocumentEntry(document);
                    });
        });
    }

    private Mono<ParseResult> parse(ExecutionInput executionInput, GraphQLSchema graphQLSchema) {
        return context(GraphQL::instrumentationState)
                .map(state -> new InstrumentationExecutionParameters(executionInput, graphQLSchema, state))
                .map(instrumentation::beginParse)
                .flatMap(ctx -> Mono.fromCallable(() -> new Parser().parseDocument(executionInput.getQuery()))
                                    .transform(ctx::instrument))
                .map(ParseResult::of)
                .onErrorResume(ParseCancellationException.class,
                               e -> Mono.just(ParseResult.ofError(e)));
    }

    private Mono<List<ValidationError>> validate(ExecutionInput executionInput, Document document, GraphQLSchema graphQLSchema) {
        return context(GraphQL::instrumentationState)
                .map(state -> new InstrumentationValidationParameters(executionInput, document, graphQLSchema, state))
                .map(instrumentation::beginValidation)
                .flatMap(ctx -> Mono.fromCallable(() -> new Validator().validateDocument(graphQLSchema, document))
                                    .transform(ctx::instrument));
    }

    private Mono<ExecutionResult> execute(ExecutionInput executionInput, Document document, GraphQLSchema graphQLSchema) {
        ExecutionId executionId = idProvider.provide(executionInput.getQuery(),
                                                     executionInput.getOperationName(),
                                                     executionInput.getContext());

        log.debug("Executing '{}'. operation name: '{}'. query: '{}'. variables '{}'", executionId,
                  executionInput.getOperationName(), executionInput.getQuery(), executionInput.getVariables());
        return execution
                .execute(document, graphQLSchema, executionId, executionInput, null)
                .doOnError(t -> log.error(
                        "Execution '{}' threw exception when executing : query : '{}'. variables '{}'",
                        executionId, executionInput.getQuery(), executionInput.getVariables(),
                        t))
                .doOnSuccess(result -> {
                    int errorCount = result.getErrors().size();
                    if (errorCount > 0) {
                        log.debug("Execution '{}' completed with '{}' errors", executionId, errorCount);
                    } else {
                        log.debug("Execution '{}' completed with zero errors", executionId);
                    }
                });
    }

    @PublicApi
    public static class Builder {
        private GraphQLSchema graphQLSchema;
        private ExecutionStrategy queryExecutionStrategy = new AsyncExecutionStrategy();
        private ExecutionStrategy mutationExecutionStrategy = new AsyncSerialExecutionStrategy();
        private ExecutionStrategy subscriptionExecutionStrategy = new SubscriptionExecutionStrategy();
        private ExecutionIdProvider idProvider = DEFAULT_EXECUTION_ID_PROVIDER;
        private Instrumentation instrumentation = SimpleInstrumentation.INSTANCE;
        private PreparsedDocumentProvider preparsedDocumentProvider = NoOpPreparsedDocumentProvider.INSTANCE;


        public Builder(GraphQLSchema graphQLSchema) {
            this.graphQLSchema = graphQLSchema;
        }

        public Builder schema(GraphQLSchema graphQLSchema) {
            this.graphQLSchema = assertNotNull(graphQLSchema, "GraphQLSchema must be non null");
            return this;
        }

        public Builder queryExecutionStrategy(ExecutionStrategy executionStrategy) {
            this.queryExecutionStrategy = assertNotNull(executionStrategy, "Query ExecutionStrategy must be non null");
            return this;
        }

        public Builder mutationExecutionStrategy(ExecutionStrategy executionStrategy) {
            this.mutationExecutionStrategy = assertNotNull(executionStrategy,
                                                           "Mutation ExecutionStrategy must be non null");
            return this;
        }

        public Builder subscriptionExecutionStrategy(ExecutionStrategy executionStrategy) {
            this.subscriptionExecutionStrategy = assertNotNull(executionStrategy,
                                                               "Subscription ExecutionStrategy must be non null");
            return this;
        }

        public Builder instrumentation(Instrumentation instrumentation) {
            this.instrumentation = assertNotNull(instrumentation, "Instrumentation must be non null");
            return this;
        }

        public Builder preparsedDocumentProvider(PreparsedDocumentProvider preparsedDocumentProvider) {
            this.preparsedDocumentProvider = assertNotNull(preparsedDocumentProvider,
                                                           "PreparsedDocumentProvider must be non null");
            return this;
        }

        public Builder executionIdProvider(ExecutionIdProvider executionIdProvider) {
            this.idProvider = assertNotNull(executionIdProvider, "ExecutionIdProvider must be non null");
            return this;
        }

        public GraphQL build() {
            assertNotNull(graphQLSchema, "graphQLSchema must be non null");
            assertNotNull(queryExecutionStrategy, "queryStrategy must be non null");
            assertNotNull(idProvider, "idProvider must be non null");
            return new GraphQL(graphQLSchema, queryExecutionStrategy, mutationExecutionStrategy,
                               subscriptionExecutionStrategy, idProvider, instrumentation, preparsedDocumentProvider);
        }
    }

    private static class ParseResult {
        private final Document document;
        private final Exception exception;

        private ParseResult(Document document, Exception exception) {
            this.document = document;
            this.exception = exception;
        }

        private static ParseResult of(Document document) {
            return new ParseResult(document, null);
        }

        private static ParseResult ofError(Exception e) {
            return new ParseResult(null, e);
        }

        private boolean isFailure() {
            return document == null;
        }

        private Document getDocument() {
            return document;
        }

        private Exception getException() {
            return exception;
        }
    }
}
