package graphql.execution.instrumentation;

import reactor.core.publisher.Mono;

/**
 * When a {@link Instrumentation}.'beginXXX()' method is called then it must return a non null InstrumentationContext
 * that will be invoked when the step is first dispatched and then when it completes.  Sometimes this is effectively the same time
 * whereas at other times its when an asynchronous {@link java.util.concurrent.CompletableFuture} completes.
 *
 * This pattern of construction of an object then call back is intended to allow "timers" to be created that can instrument what has
 * just happened or "loggers" to be called to record what has happened.
 */
public interface InstrumentationContext<T> {

    /**
     * This is invoked when the instrumentation step is initially dispatched
     *
     * @param result the result of the step as a completable future
     */
    default Mono<T> onDispatched(Mono<T> result) {
        return result;
    };

    /**
     * This is invoked when the instrumentation step is fully completed
     *
     * @param result the result of the step (which may be null)
     * @param t      this exception will be non null if an exception was thrown during the step
     */
    // TODO check if any impls depend on non-null result &and& t
    void onCompleted(T result, Throwable t);

    default Mono<T> instrument(Mono<T> input) {
        return input.transform(this::onDispatched)
                    .doOnSuccessOrError(this::onCompleted);
    }

}
