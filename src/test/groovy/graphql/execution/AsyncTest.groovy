package graphql.execution

import spock.lang.Specification

import static java.util.concurrent.CompletableFuture.completedFuture

class AsyncTest extends Specification {


    def "each works for list of futures "() {
        given:
        completedFuture('x')

        when:
        def result = Async.each([completedFuture('x'), completedFuture('y'), completedFuture('z')])

        then:
        result.isDone()
        result.get() == ['x', 'y', 'z']
    }
}
