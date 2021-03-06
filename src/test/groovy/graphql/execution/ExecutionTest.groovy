package graphql.execution

import graphql.ExecutionInput
import graphql.ExecutionResult
import graphql.ExecutionResultImpl
import graphql.MutationSchema
import graphql.execution.instrumentation.InstrumentationState
import graphql.execution.instrumentation.SimpleInstrumentation
import graphql.parser.Parser
import reactor.core.publisher.Mono
import reactor.util.context.Context
import spock.lang.Specification

import static java.util.Collections.emptyList

class ExecutionTest extends Specification {

    class CountingExecutionStrategy extends ExecutionStrategy {
        int execute = 0


        @Override
        Mono<ExecutionResult> execute(ExecutionContext executionContext, ExecutionStrategyParameters parameters) throws NonNullableFieldWasNullException {
            execute++
            return Mono.just(result())
        }

        private ExecutionResultImpl result() {
            new ExecutionResultImpl(emptyList())
        }
    }

    def parser = new Parser()
    def subscriptionStrategy = new CountingExecutionStrategy()
    def mutationStrategy = new CountingExecutionStrategy()
    def queryStrategy = new CountingExecutionStrategy()
    def execution = new Execution(queryStrategy, mutationStrategy, subscriptionStrategy, SimpleInstrumentation.INSTANCE)
    def emptyExecutionInput = ExecutionInput.newExecutionInput().build()
    def instrumentationState = new InstrumentationState() {}

    def "query strategy is used for query requests"() {
        given:
        def mutationStrategy = new CountingExecutionStrategy()

        def queryStrategy = new CountingExecutionStrategy()
        def execution = new Execution(queryStrategy, mutationStrategy, subscriptionStrategy, SimpleInstrumentation.INSTANCE)

        def query = '''
            query {
                numberHolder {
                    theNumber
                }
            }
        '''
        def document = parser.parseDocument(query)

        when:
        execution.execute(document, MutationSchema.schema, ExecutionId.generate(), emptyExecutionInput)
                .subscriberContext(Context.of(InstrumentationState.class, instrumentationState))
                .block()

        then:
        queryStrategy.execute == 1
        mutationStrategy.execute == 0
        subscriptionStrategy.execute == 0
    }

    def "mutation strategy is used for mutation requests"() {
        given:
        def query = '''
            mutation {
                changeTheNumber(newNumber: 1) {
                    theNumber
                }
            }
        '''
        def document = parser.parseDocument(query)

        when:
        execution.execute(document, MutationSchema.schema, ExecutionId.generate(), emptyExecutionInput)
                .subscriberContext(Context.of(InstrumentationState.class, instrumentationState))
                .block()

        then:
        queryStrategy.execute == 0
        mutationStrategy.execute == 1
        subscriptionStrategy.execute == 0
    }

    def "subscription strategy is used for subscription requests"() {
        given:
        def query = '''
            subscription {
                numberChanged(newNumber: 1) {
                    theNumber
                }
            }
        '''
        def document = parser.parseDocument(query)

        when:
        execution.execute(document, MutationSchema.schema, ExecutionId.generate(), emptyExecutionInput)
                .subscriberContext(Context.of(InstrumentationState.class, instrumentationState))
                .block()

        then:
        queryStrategy.execute == 0
        mutationStrategy.execute == 0
        subscriptionStrategy.execute == 1
    }
}
