package graphql.execution

import graphql.ErrorType
import graphql.ExecutionResult
import graphql.execution.instrumentation.ExecutionStrategyInstrumentationContext
import graphql.execution.instrumentation.SimpleInstrumentation
import graphql.execution.instrumentation.parameters.InstrumentationExecutionStrategyParameters
import graphql.language.Field
import graphql.language.OperationDefinition
import graphql.parser.Parser
import graphql.schema.DataFetcher
import graphql.schema.GraphQLFieldDefinition
import graphql.schema.GraphQLSchema
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier

import static graphql.Scalars.GraphQLString
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import static graphql.schema.GraphQLObjectType.newObject
import static graphql.schema.GraphQLSchema.newSchema

class AsyncExecutionStrategyTest extends Specification {

    GraphQLSchema schema(DataFetcher dataFetcher1, DataFetcher dataFetcher2) {
        GraphQLFieldDefinition.Builder fieldDefinition = newFieldDefinition()
                .name("hello")
                .type(GraphQLString)
                .dataFetcher(dataFetcher1)
        GraphQLFieldDefinition.Builder fieldDefinition2 = newFieldDefinition()
                .name("hello2")
                .type(GraphQLString)
                .dataFetcher(dataFetcher2)

        GraphQLSchema schema = newSchema().query(
                newObject()
                        .name("RootQueryType")
                        .field(fieldDefinition)
                        .field(fieldDefinition2)
                        .build()
        ).build()
        schema
    }


    def "execution is serial if the dataFetchers are blocking"() {
        given:
        def lock = new ReentrantLock()
        def counter = new AtomicInteger()
        GraphQLSchema schema = schema(
                { env ->
                    assert lock.tryLock()
                    Thread.sleep(100)
                    def result = "world" + (counter.incrementAndGet())
                    lock.unlock()
                    result
                },
                { env ->
                    assert lock.tryLock()
                    def result = "world" + (counter.incrementAndGet())
                    lock.unlock()
                    result
                }
        )
        String query = "{hello, hello2}"
        def document = new Parser().parseDocument(query)
        def operation = document.definitions[0] as OperationDefinition

        def typeInfo = ExecutionTypeInfo.newTypeInfo()
                .type(schema.getQueryType())
                .build()

        ExecutionContext executionContext = new ExecutionContextBuilder()
                .graphQLSchema(schema)
                .executionId(ExecutionId.generate())
                .operationDefinition(operation)
                .instrumentation(SimpleInstrumentation.INSTANCE)
                .build()
        ExecutionStrategyParameters executionStrategyParameters = ExecutionStrategyParameters
                .newParameters()
                .typeInfo(typeInfo)
                .fields(['hello': [Field.newField('hello').build()], 'hello2': [Field.newField('hello2').build()]])
                .build()

        AsyncExecutionStrategy asyncExecutionStrategy = new AsyncExecutionStrategy()
        when:
        def result = asyncExecutionStrategy.execute(executionContext, executionStrategyParameters)


        then:
        result.block().data == ['hello': 'world1', 'hello2': 'world2']
    }

    def "execution with already completed futures"() {
        given:

        GraphQLSchema schema = schema(
                { env -> CompletableFuture.completedFuture("world") },
                { env -> CompletableFuture.completedFuture("world2") }
        )
        String query = "{hello, hello2}"
        def document = new Parser().parseDocument(query)
        def operation = document.definitions[0] as OperationDefinition

        def typeInfo = ExecutionTypeInfo.newTypeInfo()
                .type(schema.getQueryType())
                .build()

        ExecutionContext executionContext = new ExecutionContextBuilder()
                .graphQLSchema(schema)
                .executionId(ExecutionId.generate())
                .operationDefinition(operation)
                .instrumentation(SimpleInstrumentation.INSTANCE)
                .build()
        ExecutionStrategyParameters executionStrategyParameters = ExecutionStrategyParameters
                .newParameters()
                .typeInfo(typeInfo)
                .fields(['hello': [Field.newField('hello').build()], 'hello2': [Field.newField('hello2').build()]])
                .build()

        AsyncExecutionStrategy asyncExecutionStrategy = new AsyncExecutionStrategy()
        when:
        def result = asyncExecutionStrategy.execute(executionContext, executionStrategyParameters)

        then:
        StepVerifier.create(result)
                .assertNext({ d -> assert d.data == ['hello': 'world', 'hello2': 'world2'] })
                .expectComplete()
                .verify(Duration.ofMillis(10))
    }

    def "async execution"() {
        GraphQLSchema schema = schema(
                { env -> Mono.just("world") },
                { env -> Mono.just("world2").delaySubscription(Duration.ofMillis(100))
                }
        )
        String query = "{hello, hello2}"
        def document = new Parser().parseDocument(query)
        def operation = document.definitions[0] as OperationDefinition

        def typeInfo = ExecutionTypeInfo.newTypeInfo()
                .type(schema.getQueryType())
                .build()

        ExecutionContext executionContext = new ExecutionContextBuilder()
                .graphQLSchema(schema)
                .executionId(ExecutionId.generate())
                .operationDefinition(operation)
                .instrumentation(SimpleInstrumentation.INSTANCE)
                .build()
        ExecutionStrategyParameters executionStrategyParameters = ExecutionStrategyParameters
                .newParameters()
                .typeInfo(typeInfo)
                .fields(['hello': [Field.newField('hello').build()], 'hello2': [Field.newField('hello2').build()]])
                .build()

        AsyncExecutionStrategy asyncExecutionStrategy = new AsyncExecutionStrategy()

        when:
        Supplier<Mono<ExecutionResult>> scenarioSupplier = { ->
            asyncExecutionStrategy.execute(executionContext, executionStrategyParameters)
        }

        then:
        StepVerifier.withVirtualTime(scenarioSupplier)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(100))
                .assertNext({ result -> assert result.data == ['hello': 'world', 'hello2': 'world2'] })
                .verifyComplete()
    }

    def "exception while fetching data"() {
        GraphQLSchema schema = schema(
                { env -> CompletableFuture.completedFuture("world") },
                { env ->
                    throw new NullPointerException()
                }
        )
        String query = "{hello, hello2}"
        def document = new Parser().parseDocument(query)
        def operation = document.definitions[0] as OperationDefinition

        def typeInfo = ExecutionTypeInfo.newTypeInfo()
                .type(schema.getQueryType())
                .build()

        ExecutionContext executionContext = new ExecutionContextBuilder()
                .graphQLSchema(schema)
                .executionId(ExecutionId.generate())
                .operationDefinition(operation)
                .instrumentation(SimpleInstrumentation.INSTANCE)
                .build()
        ExecutionStrategyParameters executionStrategyParameters = ExecutionStrategyParameters
                .newParameters()
                .typeInfo(typeInfo)
                .fields(['hello': [Field.newField('hello').build()], 'hello2': [Field.newField('hello2').build()]])
                .build()

        AsyncExecutionStrategy asyncExecutionStrategy = new AsyncExecutionStrategy()
        when:
        def result = asyncExecutionStrategy.execute(executionContext, executionStrategyParameters)


        then:
        StepVerifier.create(result)
                .assertNext({ d ->
            assert d.data == ['hello': 'world', 'hello2': null]
            assert d.getErrors().size() == 1
            assert d.getErrors().get(0).errorType == ErrorType.DataFetchingException
        })
                .expectComplete()
                .verify(Duration.ofMillis(10))
    }

    def "exception in instrumentation while combining data"() {
        GraphQLSchema schema = schema(
                { env -> CompletableFuture.completedFuture("world") },
                { env -> CompletableFuture.completedFuture("world2") }
        )
        String query = "{hello, hello2}"
        def document = new Parser().parseDocument(query)
        def operation = document.definitions[0] as OperationDefinition

        def typeInfo = ExecutionTypeInfo.newTypeInfo()
                .type(schema.getQueryType())
                .build()

        ExecutionContext executionContext = new ExecutionContextBuilder()
                .graphQLSchema(schema)
                .executionId(ExecutionId.generate())
                .operationDefinition(operation)
                .instrumentation(new SimpleInstrumentation() {
            @Override
            ExecutionStrategyInstrumentationContext beginExecutionStrategy(InstrumentationExecutionStrategyParameters parameters) {
                return new ExecutionStrategyInstrumentationContext() {

                    @Override
                    void onFieldValuesInfo(List<FieldValueInfo> fieldValueInfoList) {
                        throw new RuntimeException("Exception raised from instrumentation")
                    }

                    @Override
                    void onCompleted(ExecutionResult result, Throwable t) {

                    }
                }
            }
        })
                .build()
        ExecutionStrategyParameters executionStrategyParameters = ExecutionStrategyParameters
                .newParameters()
                .typeInfo(typeInfo)
                .fields(['hello': [new Field('hello')], 'hello2': [new Field('hello2')]])
                .build()

        AsyncExecutionStrategy asyncExecutionStrategy = new AsyncExecutionStrategy()
        when:
        def result = asyncExecutionStrategy.execute(executionContext, executionStrategyParameters)

        then:
        StepVerifier.create(result)
                .expectErrorMessage("Exception raised from instrumentation")
                .verify()
    }


}
