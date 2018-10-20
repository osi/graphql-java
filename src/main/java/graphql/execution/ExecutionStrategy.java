package graphql.execution;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.Internal;
import graphql.PublicSpi;
import graphql.SerializationError;
import graphql.TypeMismatchError;
import graphql.TypeResolutionEnvironment;
import graphql.UnresolvedTypeError;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationFieldCompleteParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldParameters;
import graphql.introspection.Introspection;
import graphql.language.Field;
import graphql.schema.CoercingSerializeException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.DataFetchingFieldSelectionSetImpl;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;
import graphql.schema.visibility.GraphqlFieldVisibility;
import graphql.util.FpKit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static graphql.execution.ExecutionTypeInfo.newTypeInfo;
import static graphql.execution.FieldCollectorParameters.newParameters;
import static graphql.execution.FieldValueInfo.CompleteValueType.ENUM;
import static graphql.execution.FieldValueInfo.CompleteValueType.LIST;
import static graphql.execution.FieldValueInfo.CompleteValueType.NULL;
import static graphql.execution.FieldValueInfo.CompleteValueType.OBJECT;
import static graphql.execution.FieldValueInfo.CompleteValueType.SCALAR;
import static graphql.schema.DataFetchingEnvironmentBuilder.newDataFetchingEnvironment;
import static graphql.schema.GraphQLTypeUtil.isList;

/**
 * An execution strategy is give a list of fields from the graphql query to execute and find values for using a recursive strategy.
 * <pre>
 *     query {
 *          friends {
 *              id
 *              name
 *              friends {
 *                  id
 *                  name
 *              }
 *          }
 *          enemies {
 *              id
 *              name
 *              allies {
 *                  id
 *                  name
 *              }
 *          }
 *     }
 *
 * </pre>
 * <p>
 * Given the graphql query above, an execution strategy will be called for the top level fields 'friends' and 'enemies' and it will be asked to find an object
 * to describe them.  Because they are both complex object types, it needs to descend down that query and start fetching and completing
 * fields such as 'id','name' and other complex fields such as 'friends' and 'allies', by recursively calling to itself to execute these lower
 * field layers
 * <p>
 * The execution of a field has two phases, first a raw object must be fetched for a field via a {@link DataFetcher} which
 * is defined on the {@link GraphQLFieldDefinition}.  This object must then be 'completed' into a suitable value, either as a scalar/enum type via
 * coercion or if its a complex object type by recursively calling the execution strategy for the lower level fields.
 * <p>
 * The first phase (data fetching) is handled by the method {@link #fetchField(ExecutionContext, ExecutionStrategyParameters)}
 * <p>
 * The second phase (value completion) is handled by the methods {@link #completeField(ExecutionContext, ExecutionStrategyParameters, Object)}
 * and the other "completeXXX" methods.
 * <p>
 * The order of fields fetching and completion is up to the execution strategy. As the graphql specification
 * <a href="http://facebook.github.io/graphql/#sec-Normal-and-Serial-Execution">http://facebook.github.io/graphql/#sec-Normal-and-Serial-Execution</a> says:
 * <blockquote>
 * Normally the executor can execute the entries in a grouped field set in whatever order it chooses (often in parallel). Because
 * the resolution of fields other than top-level mutation fields must always be side effect-free and idempotent, the
 * execution order must not affect the result, and hence the server has the freedom to execute the
 * field entries in whatever order it deems optimal.
 * </blockquote>
 * <p>
 * So in the case above you could execute the fields depth first ('friends' and its sub fields then do 'enemies' and its sub fields or it
 * could do breadth first ('fiends' and 'enemies' data fetch first and then all the sub fields) or in parallel via asynchronous
 * facilities like {@link CompletableFuture}s.
 * <p>
 * {@link #execute(ExecutionContext, ExecutionStrategyParameters)} is the entry point of the execution strategy.
 */
@PublicSpi
public abstract class ExecutionStrategy {

    private static final Logger log = LoggerFactory.getLogger(ExecutionStrategy.class);

    protected final ValuesResolver valuesResolver = new ValuesResolver();
    protected final FieldCollector fieldCollector = new FieldCollector();

    protected final DataFetcherExceptionHandler dataFetcherExceptionHandler;

    /**
     * The default execution strategy constructor uses the {@link SimpleDataFetcherExceptionHandler}
     * for data fetching errors.
     */
    protected ExecutionStrategy() {
        dataFetcherExceptionHandler = new SimpleDataFetcherExceptionHandler();
    }

    /**
     * The consumers of the execution strategy can pass in a {@link DataFetcherExceptionHandler} to better
     * decide what do when a data fetching error happens
     *
     * @param dataFetcherExceptionHandler the callback invoked if an exception happens during data fetching
     */
    protected ExecutionStrategy(DataFetcherExceptionHandler dataFetcherExceptionHandler) {
        this.dataFetcherExceptionHandler = dataFetcherExceptionHandler;
    }

    @Internal
    public static String mkNameForPath(Field currentField) {
        return mkNameForPath(Collections.singletonList(currentField));
    }

    @Internal
    public static String mkNameForPath(List<Field> currentField) {
        Field field = currentField.get(0);
        return field.getAlias() != null ? field.getAlias() : field.getName();
    }

    /**
     * This is the entry point to an execution strategy.  It will be passed the fields to execute and get values for.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to an {@link ExecutionResult}
     *
     * @throws NonNullableFieldWasNullException in the future if a non null field resolves to a null value
     */
    public abstract Mono<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException;

    /**
     * Called to fetch a value for a field and resolve it further in terms of the graphql query.  This will call
     * #fetchField followed by #completeField and the completed {@link ExecutionResult} is returned.
     * <p>
     * An execution strategy can iterate the fields to be executed and call this method for each one
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to an {@link ExecutionResult}
     *
     * @throws NonNullableFieldWasNullException in the future if a non null field resolves to a null value
     */
    protected Mono<ExecutionResult> resolveField(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        return resolveFieldWithInfo(executionContext, parameters).flatMap(FieldValueInfo::getFieldValue);
    }

    /**
     * Called to fetch a value for a field and its extra runtime info and resolve it further in terms of the graphql query.  This will call
     * #fetchField followed by #completeField and the completed {@link graphql.execution.FieldValueInfo} is returned.
     * <p>
     * An execution strategy can iterate the fields to be executed and call this method for each one
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to a {@link FieldValueInfo}
     *
     * @throws NonNullableFieldWasNullException in the {@link FieldValueInfo#getFieldValue()} future if a non null field resolves to a null value
     */
    protected Mono<FieldValueInfo> resolveFieldWithInfo(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext, parameters, parameters.getField().get(0));

        InstrumentationContext<ExecutionResult> fieldCtx =
                executionContext.getInstrumentation()
                                .beginField(new InstrumentationFieldParameters(executionContext, fieldDef,
                                                                               fieldTypeInfo(parameters, fieldDef)));

        Mono<FieldValueInfo> result = fetchField(executionContext, parameters)
                .flatMap((fetchedValue) -> completeField(executionContext, parameters, fetchedValue));

        // TODO this is never subscribed to - figure out why - perhaps push this into the getFieldValue mono
        Mono<ExecutionResult> executionResultFuture = result.flatMap(FieldValueInfo::getFieldValue)
                                                            .transform(fieldCtx::instrument);

        return result;
    }

    /**
     * Called to fetch a value for a field from the {@link DataFetcher} associated with the field
     * {@link GraphQLFieldDefinition}.
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a promise to a fetched object
     *
     * @throws NonNullableFieldWasNullException in the future if a non null field resolves to a null value
     */
    protected Mono<Object> fetchField(ExecutionContext executionContext, ExecutionStrategyParameters parameters) {
        Field field = parameters.getField().get(0);
        GraphQLObjectType parentType = parameters.getTypeInfo().castType(GraphQLObjectType.class);
        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext.getGraphQLSchema(), parentType, field);

        GraphqlFieldVisibility fieldVisibility = executionContext.getGraphQLSchema().getFieldVisibility();
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(fieldVisibility, fieldDef.getArguments(),
                                                                              field.getArguments(),
                                                                              executionContext.getVariables());

        GraphQLOutputType fieldType = fieldDef.getType();
        DataFetchingFieldSelectionSet fieldCollector = DataFetchingFieldSelectionSetImpl.newCollector(executionContext,
                                                                                                      fieldType,
                                                                                                      parameters.getField());
        ExecutionTypeInfo fieldTypeInfo = fieldTypeInfo(parameters, fieldDef);

        DataFetchingEnvironment environment = newDataFetchingEnvironment(executionContext)
                .source(parameters.getSource())
                .arguments(argumentValues)
                .fieldDefinition(fieldDef)
                .fields(parameters.getField())
                .fieldType(fieldType)
                .fieldTypeInfo(fieldTypeInfo)
                .parentType(parentType)
                .selectionSet(fieldCollector)
                .build();

        Instrumentation instrumentation = executionContext.getInstrumentation();

        InstrumentationFieldFetchParameters instrumentationFieldFetchParams = new InstrumentationFieldFetchParameters(
                executionContext, fieldDef, environment, parameters);
        InstrumentationContext<Object> fetchCtx = instrumentation.beginFieldFetch(instrumentationFieldFetchParams);

        ExecutionId executionId = executionContext.getExecutionId();

        return Mono.fromCallable(() -> {
            DataFetcher dataFetcher = fieldDef.getDataFetcher();
            dataFetcher = instrumentation.instrumentDataFetcher(dataFetcher, instrumentationFieldFetchParams);
            log.debug("'{}' fetching field '{}' using data fetcher '{}'...", executionId, fieldTypeInfo.getPath(),
                      dataFetcher.getClass().getName());
            Object fetchedValueRaw = dataFetcher.get(environment);
            log.debug("'{}' field '{}' fetch returned '{}'", executionId, fieldTypeInfo.getPath(),
                      fetchedValueRaw == null ? "null" : fetchedValueRaw.getClass().getName());
            return fetchedValueRaw;
        })
                   .flatMap(Async::toMono)
                   .doOnError(t -> log.debug("'{}', field '{}' fetch threw exception", executionId,
                                             fieldTypeInfo.getPath(), t))
                   .onErrorResume(t -> true,
                                  t -> {
                                      handleFetchingException(executionContext, parameters, field,
                                                              fieldDef, argumentValues,
                                                              environment, t);
                                      return Mono.empty();
                                  }
                   )
                   .transform(fetchCtx::instrument)
                   .map(result -> unboxPossibleDataFetcherResult(executionContext, parameters, result))
                   .flatMap(this::unboxPossibleOptional);
    }

    Object unboxPossibleDataFetcherResult(ExecutionContext executionContext,
                                          ExecutionStrategyParameters parameters,
                                          Object result) {
        if (result instanceof DataFetcherResult) {
            //noinspection unchecked
            DataFetcherResult<?> dataFetcherResult = (DataFetcherResult) result;
            dataFetcherResult.getErrors().stream()
                             .map(relError -> new AbsoluteGraphQLError(parameters, relError))
                             .forEach(executionContext::addError);
            return dataFetcherResult.getData();
        } else {
            return result;
        }
    }

    private void handleFetchingException(ExecutionContext executionContext,
                                         ExecutionStrategyParameters parameters,
                                         Field field,
                                         GraphQLFieldDefinition fieldDef,
                                         Map<String, Object> argumentValues,
                                         DataFetchingEnvironment environment,
                                         Throwable e) {
        DataFetcherExceptionHandlerParameters handlerParameters = DataFetcherExceptionHandlerParameters.newExceptionParameters()
                                                                                                       .executionContext(
                                                                                                               executionContext)
                                                                                                       .dataFetchingEnvironment(
                                                                                                               environment)
                                                                                                       .argumentValues(
                                                                                                               argumentValues)
                                                                                                       .field(field)
                                                                                                       .fieldDefinition(
                                                                                                               fieldDef)
                                                                                                       .path(parameters.getPath())
                                                                                                       .exception(e)
                                                                                                       .build();

        dataFetcherExceptionHandler.accept(handlerParameters);

        parameters.deferredErrorSupport().onFetchingException(parameters, e);
    }

    /**
     * Called to complete a field based on the type of the field.
     * <p>
     * If the field is a scalar type, then it will be coerced  and returned.  However if the field type is an complex object type, then
     * the execution strategy will be called recursively again to execute the fields of that type before returning.
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param fetchedValue     the fetched raw value
     *
     * @return a {@link FieldValueInfo}
     *
     * @throws NonNullableFieldWasNullException in the {@link FieldValueInfo#getFieldValue()} future if a non null field resolves to a null value
     */
    protected Mono<FieldValueInfo> completeField(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Object fetchedValue) {
        Field field = parameters.getField().get(0);
        GraphQLObjectType parentType = parameters.getTypeInfo().castType(GraphQLObjectType.class);
        GraphQLFieldDefinition fieldDef = getFieldDef(executionContext.getGraphQLSchema(), parentType, field);
        ExecutionTypeInfo fieldTypeInfo = fieldTypeInfo(parameters, fieldDef);

        Instrumentation instrumentation = executionContext.getInstrumentation();
        InstrumentationFieldCompleteParameters instrumentationParams = new InstrumentationFieldCompleteParameters(
                executionContext, parameters, fieldDef, fieldTypeInfo, fetchedValue);
        InstrumentationContext<ExecutionResult> ctxCompleteField = instrumentation.beginFieldComplete(
                instrumentationParams
        );

        GraphqlFieldVisibility fieldVisibility = executionContext.getGraphQLSchema().getFieldVisibility();
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(fieldVisibility, fieldDef.getArguments(),
                                                                              field.getArguments(),
                                                                              executionContext.getVariables());

        NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext,
                                                                                            fieldTypeInfo);

        ExecutionStrategyParameters newParameters = parameters.transform(builder ->
                                                                                 builder.typeInfo(fieldTypeInfo)
                                                                                        .arguments(argumentValues)
                                                                                        .source(fetchedValue)
                                                                                        .nonNullFieldValidator(
                                                                                                nonNullableFieldValidator)
        );

        log.debug("'{}' completing field '{}'...", executionContext.getExecutionId(), fieldTypeInfo.getPath());

        Mono<FieldValueInfo> fieldValueInfo = completeValue(executionContext, newParameters);

        // TODO again with the not looking at the value that comes out..
        fieldValueInfo.map(fvi -> fvi.getFieldValue().transform(ctxCompleteField::instrument));

        return fieldValueInfo;
    }

    /**
     * Called to complete a value for a field based on the type of the field.
     * <p>
     * If the field is a scalar type, then it will be coerced  and returned.  However if the field type is an complex object type, then
     * the execution strategy will be called recursively again to execute the fields of that type before returning.
     * <p>
     * Graphql fragments mean that for any give logical field can have one or more {@link Field} values associated with it
     * in the query, hence the fieldList.  However the first entry is representative of the field for most purposes.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     *
     * @return a {@link FieldValueInfo}
     *
     * @throws NonNullableFieldWasNullException if a non null field resolves to a null value
     */
    protected Mono<FieldValueInfo> completeValue(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException {
        return Mono.defer(() -> unboxPossibleOptional(parameters.getSource()))
                   .flatMap(result -> {
                       GraphQLType fieldType = parameters.getTypeInfo().getType();

                       if (isList(fieldType)) {
                           return completeValueForList(executionContext, parameters, result);
                       } else if (fieldType instanceof GraphQLScalarType) {
                           Mono<ExecutionResult> fieldValue =
                                   completeValueForScalar(executionContext, parameters, (GraphQLScalarType) fieldType,
                                                          result);
                           return Mono.just(FieldValueInfo.newFieldValueInfo(SCALAR).fieldValue(fieldValue).build());
                       } else if (fieldType instanceof GraphQLEnumType) {
                           Mono<ExecutionResult> fieldValue =
                                   completeValueForEnum(executionContext, parameters, (GraphQLEnumType) fieldType,
                                                        result);
                           return Mono.just(FieldValueInfo.newFieldValueInfo(ENUM).fieldValue(fieldValue).build());
                       }

                       // when we are here, we have a complex type: Interface, Union or Object
                       // and we must go deeper
                       //
                       GraphQLObjectType resolvedObjectType;
                       Mono<ExecutionResult> fieldValue;
                       try {
                           resolvedObjectType = resolveType(executionContext, parameters, fieldType);
                           fieldValue = completeValueForObject(executionContext, parameters, resolvedObjectType,
                                                               result);
                       } catch (UnresolvedTypeException ex) {
                           // consider the result to be null and add the error on the context
                           handleUnresolvedTypeProblem(executionContext, parameters, ex);
                           // and validate the field is nullable, if non-nullable throw exception
                           parameters.getNonNullFieldValidator().validate(parameters.getPath(), null);
                           // complete the field as null
                           fieldValue = Mono.just(new ExecutionResultImpl(null, null));
                       }
                       return Mono.just(FieldValueInfo.newFieldValueInfo(OBJECT).fieldValue(fieldValue).build());
                   })
                   .switchIfEmpty(Mono.fromCallable(() -> {
                       Mono<ExecutionResult> fieldValue = completeValueForNull(parameters);
                       return FieldValueInfo.newFieldValueInfo(NULL).fieldValue(fieldValue).build();
                   }));
    }

    private void handleUnresolvedTypeProblem(ExecutionContext context, ExecutionStrategyParameters parameters, UnresolvedTypeException e) {
        UnresolvedTypeError error = new UnresolvedTypeError(parameters.getPath(), parameters.getTypeInfo(), e);
        log.warn(error.getMessage(), e);
        context.addError(error);

        parameters.deferredErrorSupport().onError(error);
    }

    private Mono<ExecutionResult> completeValueForNull(ExecutionStrategyParameters parameters) {
        return Mono.<ExecutionResult>fromCallable(
                () -> parameters.getNonNullFieldValidator().validate(parameters.getPath(), null))
                .defaultIfEmpty(new ExecutionResultImpl(null, null));
    }

    /**
     * Called to complete a list of value for a field based on a list type.  This iterates the values and calls
     * {@link #completeValue(ExecutionContext, ExecutionStrategyParameters)} for each value.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param result           the result to complete, raw result
     *
     * @return a {@link FieldValueInfo}
     */
    protected Mono<FieldValueInfo> completeValueForList(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Object result) {
        return Mono.fromCallable(() -> {
            Iterable<Object> resultIterable = toIterable(executionContext, parameters, result);
            return parameters.getNonNullFieldValidator()
                             .validate(parameters.getPath(), resultIterable);
        })
                   .flatMap(resultIterable -> completeValueForList(executionContext, parameters, resultIterable))
                   .switchIfEmpty(Mono.just(FieldValueInfo.newFieldValueInfo(LIST)
                                                          .fieldValue(Mono.just(new ExecutionResultImpl(null, null)))
                                                          .build()))
                   .onErrorResume(NonNullableFieldWasNullException.class,
                                  e -> Mono.just(FieldValueInfo.newFieldValueInfo(LIST)
                                                               .fieldValue(Mono.error(e))
                                                               .build()));

    }

    /**
     * Called to complete a list of value for a field based on a list type.  This iterates the values and calls
     * {@link #completeValue(ExecutionContext, ExecutionStrategyParameters)} for each value.
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param iterableValues   the values to complete, can't be null
     *
     * @return a {@link FieldValueInfo}
     */
    protected Mono<FieldValueInfo> completeValueForList(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Iterable<Object> iterableValues) {

        Collection<Object> values = FpKit.toCollection(iterableValues);
        ExecutionTypeInfo typeInfo = parameters.getTypeInfo();
        GraphQLList fieldType = typeInfo.castType(GraphQLList.class);
        GraphQLFieldDefinition fieldDef = parameters.getTypeInfo().getFieldDefinition();
        Field field = parameters.getTypeInfo().getField();

        InstrumentationFieldCompleteParameters instrumentationParams = new InstrumentationFieldCompleteParameters(
                executionContext, parameters, fieldDef, fieldTypeInfo(parameters, fieldDef), values);
        Instrumentation instrumentation = executionContext.getInstrumentation();

        InstrumentationContext<ExecutionResult> completeListCtx = instrumentation.beginFieldListComplete(
                instrumentationParams
        );

        return Flux.fromIterable(values)
                   .index((l, item) -> {
                       int index = Math.toIntExact(l);
                       ExecutionPath indexedPath = parameters.getPath().segment(index);

                       ExecutionTypeInfo wrappedTypeInfo = ExecutionTypeInfo.newTypeInfo()
                                                                            .parentInfo(typeInfo)
                                                                            .type(fieldType.getWrappedType())
                                                                            .path(indexedPath)
                                                                            .fieldDefinition(fieldDef)
                                                                            .field(field)
                                                                            .build();

                       NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(
                               executionContext,
                               wrappedTypeInfo);

                       ExecutionStrategyParameters newParameters = parameters.transform(builder ->
                                                                                                builder.typeInfo(
                                                                                                        wrappedTypeInfo)
                                                                                                       .nonNullFieldValidator(
                                                                                                               nonNullableFieldValidator)
                                                                                                       .listSize(
                                                                                                               values.size())
                                                                                                       .currentListIndex(
                                                                                                               index)
                                                                                                       .path(indexedPath)
                                                                                                       .source(item)
                       );
                       return completeValue(executionContext, newParameters);
                   })
                   .flatMapSequential(Function.identity())
                   .collectList()
                   .map(fieldValueInfos -> {
                       Mono<ExecutionResult> overallResult = Flux.fromIterable(fieldValueInfos)
                                                                 .flatMapSequential(
                                                                         i -> handleNonNullException(i.getFieldValue(),
                                                                                                     executionContext))
                                                                 .map(ExecutionResult::getData)
                                                                 .collectList()
                               .<ExecutionResult>map(
                                       completedResults -> new ExecutionResultImpl(completedResults, null))
                               .transform(completeListCtx::instrument);

                       return FieldValueInfo.newFieldValueInfo(LIST)
                                            .fieldValue(overallResult)
                                            .fieldValueInfos(fieldValueInfos)
                                            .build();
                   });
    }

    /**
     * Called to turn an object into a scalar value according to the {@link GraphQLScalarType} by asking that scalar type to coerce the object
     * into a valid value
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param scalarType       the type of the scalar
     * @param result           the result to be coerced
     *
     * @return a promise to an {@link ExecutionResult}
     */
    protected Mono<ExecutionResult> completeValueForScalar(ExecutionContext executionContext,
                                                           ExecutionStrategyParameters parameters,
                                                           GraphQLScalarType scalarType,
                                                           Object result) {
        return Mono.fromCallable(() -> scalarType.getCoercing().serialize(result))
                   .onErrorResume(CoercingSerializeException.class,
                                  e -> handleCoercionProblem(executionContext, parameters, e))
                   .flatMap(serialized -> {
                       // TODO: fix that: this should not be handled here
                       //6.6.1 http://facebook.github.io/graphql/#sec-Field-entries
                       if (serialized instanceof Double && ((Double) serialized).isNaN()) {
                           return Mono.empty();
                       }
                       return Mono.just(serialized);
                   })
                   // TODO this null check won't work because the value will never be null - need to handle empty
                   .map(serialized -> parameters.getNonNullFieldValidator().validate(parameters.getPath(), serialized))
                   .map(serialized -> new ExecutionResultImpl(serialized, null));
    }

    /**
     * Called to turn an object into a enum value according to the {@link GraphQLEnumType} by asking that enum type to coerce the object into a valid value
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param enumType         the type of the enum
     * @param result           the result to be coerced
     *
     * @return a promise to an {@link ExecutionResult}
     */
    protected Mono<ExecutionResult> completeValueForEnum(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLEnumType enumType, Object result) {
        return Mono.fromCallable(() -> enumType.getCoercing().serialize(result))
                   .onErrorResume(CoercingSerializeException.class,
                                  e -> handleCoercionProblem(executionContext, parameters, e))
                   // TODO null won't be handled as expected here
                   .map(serialized -> parameters.getNonNullFieldValidator().validate(parameters.getPath(), serialized))
                   .map(serialized -> new ExecutionResultImpl(serialized, null));
    }

    /**
     * Called to turn an java object value into an graphql object value
     *
     * @param executionContext   contains the top level execution parameters
     * @param parameters         contains the parameters holding the fields to be executed and source object
     * @param resolvedObjectType the resolved object type
     * @param result             the result to be coerced
     *
     * @return a promise to an {@link ExecutionResult}
     */
    protected Mono<ExecutionResult> completeValueForObject(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLObjectType resolvedObjectType, Object result) {
        ExecutionTypeInfo typeInfo = parameters.getTypeInfo();

        FieldCollectorParameters collectorParameters = newParameters()
                .schema(executionContext.getGraphQLSchema())
                .objectType(resolvedObjectType)
                .fragments(executionContext.getFragmentsByName())
                .variables(executionContext.getVariables())
                .build();

        Map<String, List<Field>> subFields = fieldCollector.collectFields(collectorParameters, parameters.getField());

        ExecutionTypeInfo newTypeInfo = typeInfo.treatAs(resolvedObjectType);
        NonNullableFieldValidator nonNullableFieldValidator = new NonNullableFieldValidator(executionContext,
                                                                                            newTypeInfo);

        ExecutionStrategyParameters newParameters = parameters.transform(builder ->
                                                                                 builder.typeInfo(newTypeInfo)
                                                                                        .fields(subFields)
                                                                                        .nonNullFieldValidator(
                                                                                                nonNullableFieldValidator)
                                                                                        .source(result)
        );

        // Calling this from the executionContext to ensure we shift back from mutation strategy to the query strategy.

        return executionContext.getQueryStrategy().execute(executionContext, newParameters);
    }

    @SuppressWarnings("SameReturnValue")
    private Mono<Object> handleCoercionProblem(ExecutionContext context, ExecutionStrategyParameters parameters, CoercingSerializeException e) {
        SerializationError error = new SerializationError(parameters.getPath(), e);
        log.warn(error.getMessage(), e);
        context.addError(error);

        parameters.deferredErrorSupport().onError(error);

        return Mono.empty();
    }

    /**
     * We treat Optional objects as "boxed" values where an empty Optional
     * equals a null object and a present Optional is the underlying value.
     *
     * @param result the incoming value
     *
     * @return an un-boxed result
     */
    protected Mono<Object> unboxPossibleOptional(Object result) {
        if (result instanceof Optional) {
            Optional optional = (Optional) result;
            return Mono.justOrEmpty(optional);
        } else if (result instanceof OptionalInt) {
            OptionalInt optional = (OptionalInt) result;
            if (optional.isPresent()) {
                return Mono.just(optional.getAsInt());
            } else {
                return Mono.empty();
            }
        } else if (result instanceof OptionalDouble) {
            OptionalDouble optional = (OptionalDouble) result;
            if (optional.isPresent()) {
                return Mono.just(optional.getAsDouble());
            } else {
                return Mono.empty();
            }
        } else if (result instanceof OptionalLong) {
            OptionalLong optional = (OptionalLong) result;
            if (optional.isPresent()) {
                return Mono.just(optional.getAsLong());
            } else {
                return Mono.empty();
            }
        }

        return Mono.just(result);
    }

    /**
     * Converts an object that is known to should be an Iterable into one
     *
     * @param result the result object
     *
     * @return an Iterable from that object
     *
     * @throws java.lang.ClassCastException if its not an Iterable
     */
    @SuppressWarnings("unchecked")
    protected Iterable<Object> toIterable(Object result) {
        return FpKit.toCollection(result);
    }

    protected GraphQLObjectType resolveType(ExecutionContext executionContext, ExecutionStrategyParameters parameters, GraphQLType fieldType) {
        GraphQLObjectType resolvedType;
        if (fieldType instanceof GraphQLInterfaceType) {
            TypeResolutionParameters resolutionParams = TypeResolutionParameters.newParameters()
                                                                                .graphQLInterfaceType(
                                                                                        (GraphQLInterfaceType) fieldType)
                                                                                .field(parameters.getField().get(0))
                                                                                .value(parameters.getSource())
                                                                                .argumentValues(
                                                                                        parameters.getArguments())
                                                                                .context(executionContext.getContext())
                                                                                .schema(executionContext.getGraphQLSchema()).build();
            resolvedType = resolveTypeForInterface(resolutionParams);

        } else if (fieldType instanceof GraphQLUnionType) {
            TypeResolutionParameters resolutionParams = TypeResolutionParameters.newParameters()
                                                                                .graphQLUnionType(
                                                                                        (GraphQLUnionType) fieldType)
                                                                                .field(parameters.getField().get(0))
                                                                                .value(parameters.getSource())
                                                                                .argumentValues(
                                                                                        parameters.getArguments())
                                                                                .context(executionContext.getContext())
                                                                                .schema(executionContext.getGraphQLSchema()).build();
            resolvedType = resolveTypeForUnion(resolutionParams);
        } else {
            resolvedType = (GraphQLObjectType) fieldType;
        }

        return resolvedType;
    }

    /**
     * Called to resolve a {@link GraphQLInterfaceType} into a specific {@link GraphQLObjectType} so the object can be executed in terms of that type
     *
     * @param params the params needed for type resolution
     *
     * @return a {@link GraphQLObjectType}
     */
    protected GraphQLObjectType resolveTypeForInterface(TypeResolutionParameters params) {
        TypeResolutionEnvironment env = new TypeResolutionEnvironment(params.getValue(), params.getArgumentValues(),
                                                                      params.getField(),
                                                                      params.getGraphQLInterfaceType(),
                                                                      params.getSchema(), params.getContext());
        GraphQLInterfaceType abstractType = params.getGraphQLInterfaceType();
        GraphQLObjectType result = abstractType.getTypeResolver().getType(env);
        if (result == null) {
            throw new UnresolvedTypeException(abstractType);
        }

        if (!params.getSchema().isPossibleType(abstractType, result)) {
            throw new UnresolvedTypeException(abstractType, result);
        }

        return result;
    }

    /**
     * Called to resolve a {@link GraphQLUnionType} into a specific {@link GraphQLObjectType} so the object can be executed in terms of that type
     *
     * @param params the params needed for type resolution
     *
     * @return a {@link GraphQLObjectType}
     */
    protected GraphQLObjectType resolveTypeForUnion(TypeResolutionParameters params) {
        TypeResolutionEnvironment env = new TypeResolutionEnvironment(params.getValue(), params.getArgumentValues(),
                                                                      params.getField(), params.getGraphQLUnionType(),
                                                                      params.getSchema(), params.getContext());
        GraphQLUnionType abstractType = params.getGraphQLUnionType();
        GraphQLObjectType result = abstractType.getTypeResolver().getType(env);
        if (result == null) {
            throw new UnresolvedTypeException(abstractType);
        }

        if (!params.getSchema().isPossibleType(abstractType, result)) {
            throw new UnresolvedTypeException(abstractType, result);
        }

        return result;
    }

    protected Iterable<Object> toIterable(ExecutionContext context, ExecutionStrategyParameters parameters, Object result) {
        if (result.getClass().isArray() || result instanceof Iterable) {
            return toIterable(result);
        }

        handleTypeMismatchProblem(context, parameters, result);
        return null;
    }

    private void handleTypeMismatchProblem(ExecutionContext context, ExecutionStrategyParameters parameters, Object result) {
        TypeMismatchError error = new TypeMismatchError(parameters.getPath(), parameters.getTypeInfo().getType());
        log.warn("{} got {}", error.getMessage(), result.getClass());
        context.addError(error);

        parameters.deferredErrorSupport().onError(error);
    }

    /**
     * Called to discover the field definition give the current parameters and the AST {@link Field}
     *
     * @param executionContext contains the top level execution parameters
     * @param parameters       contains the parameters holding the fields to be executed and source object
     * @param field            the field to find the definition of
     *
     * @return a {@link GraphQLFieldDefinition}
     */
    protected GraphQLFieldDefinition getFieldDef(ExecutionContext executionContext, ExecutionStrategyParameters parameters, Field field) {
        GraphQLObjectType parentType = parameters.getTypeInfo().castType(GraphQLObjectType.class);
        return getFieldDef(executionContext.getGraphQLSchema(), parentType, field);
    }

    /**
     * Called to discover the field definition give the current parameters and the AST {@link Field}
     *
     * @param schema     the schema in play
     * @param parentType the parent type of the field
     * @param field      the field to find the definition of
     *
     * @return a {@link GraphQLFieldDefinition}
     */
    protected GraphQLFieldDefinition getFieldDef(GraphQLSchema schema, GraphQLObjectType parentType, Field field) {
        return Introspection.getFieldDef(schema, parentType, field.getName());
    }

    /**
     * See (http://facebook.github.io/graphql/#sec-Errors-and-Non-Nullability),
     * <p>
     * If a non nullable child field type actually resolves to a null value and the parent type is nullable
     * then the parent must in fact become null
     * so we use exceptions to indicate this special case.  However if the parent is in fact a non nullable type
     * itself then we need to bubble that upwards again until we get to the root in which case the result
     * is meant to be null.
     *
     * @param e this indicates that a null value was returned for a non null field, which needs to cause the parent field
     *          to become null OR continue on as an exception
     *
     * @throws NonNullableFieldWasNullException if a non null field resolves to a null value
     */
    protected <T> Mono<T> assertNonNullFieldPrecondition(NonNullableFieldWasNullException e) throws NonNullableFieldWasNullException {
        ExecutionTypeInfo typeInfo = e.getTypeInfo();
        if (typeInfo.hasParentType() && typeInfo.getParentTypeInfo().isNonNullType()) {
            return Mono.error(new NonNullableFieldWasNullException(e));
        }
        return Mono.empty();
    }

    protected void assertNonNullFieldPrecondition(NonNullableFieldWasNullException e, CompletableFuture<?> completableFuture) throws NonNullableFieldWasNullException {
        ExecutionTypeInfo typeInfo = e.getTypeInfo();
        if (typeInfo.hasParentType() && typeInfo.getParentTypeInfo().isNonNullType()) {
            completableFuture.completeExceptionally(new NonNullableFieldWasNullException(e));
        }
    }

    protected Mono<ExecutionResult> handleNonNullException(Mono<ExecutionResult> result, ExecutionContext executionContext) {
        return result
                .onErrorMap(CompletionException.class, Throwable::getCause)
                .onErrorMap(t -> {
                    if (t instanceof NonNullableFieldWasNullException) {
                        ExecutionTypeInfo typeInfo = ((NonNullableFieldWasNullException) t).getTypeInfo();
                        return typeInfo.hasParentType() && typeInfo.getParentTypeInfo().isNonNullType();
                    }
                    return false;
                }, t -> new NonNullableFieldWasNullException((NonNullableFieldWasNullException) t))
                .onErrorResume(AbortExecutionException.class, ae -> Mono.just(ae.toExecutionResult()))
                .onErrorResume(NonNullableFieldWasNullException.class,
                               t -> Mono.just(new ExecutionResultImpl(null, executionContext.getErrors())));
    }

    /**
     * Builds the type info hierarchy for the current field
     *
     * @param parameters      contains the parameters holding the fields to be executed and source object
     * @param fieldDefinition the field definition to build type info for
     *
     * @return a new type info
     */
    protected ExecutionTypeInfo fieldTypeInfo(ExecutionStrategyParameters parameters, GraphQLFieldDefinition fieldDefinition) {
        GraphQLOutputType fieldType = fieldDefinition.getType();
        Field field = null;
        if (parameters.getField() != null && !parameters.getField().isEmpty()) {
            field = parameters.getField().get(0);
        }
        return newTypeInfo()
                .type(fieldType)
                .fieldDefinition(fieldDefinition)
                .field(field)
                .path(parameters.getPath())
                .parentInfo(parameters.getTypeInfo())
                .build();

    }
}
