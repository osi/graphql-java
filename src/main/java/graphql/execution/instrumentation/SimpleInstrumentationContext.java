package graphql.execution.instrumentation;

import graphql.PublicApi;
import reactor.core.publisher.Mono;

import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

/**
 * A simple implementation of {@link InstrumentationContext}
 */
@PublicApi
public class SimpleInstrumentationContext<T> implements InstrumentationContext<T> {

    private final BiConsumer<T, Throwable> codeToRunOnComplete;
    private final UnaryOperator<Mono<T>> codeToRunOnDispatch;

    public SimpleInstrumentationContext() {
        this(null, null);
    }

    private SimpleInstrumentationContext(UnaryOperator<Mono<T>> codeToRunOnDispatch, BiConsumer<T, Throwable> codeToRunOnComplete) {
        this.codeToRunOnComplete = codeToRunOnComplete;
        this.codeToRunOnDispatch = codeToRunOnDispatch;
    }

    @Override
    public Mono<T> onDispatched(Mono<T> result) {
        if (codeToRunOnDispatch == null) {
            return result;
        }
        return result.transform(codeToRunOnDispatch);
    }

    @Override
    public void onCompleted(T result, Throwable t) {
        if (codeToRunOnComplete != null) {
            codeToRunOnComplete.accept(result, t);
        }
    }

    /**
     * Allows for the more fluent away to return an instrumentation context that runs the specified
     * code on instrumentation step dispatch.
     *
     * @param codeToRun the code to run on dispatch
     * @param <U>       the generic type
     *
     * @return an instrumentation context
     */
    public static <U> SimpleInstrumentationContext<U> whenDispatched(UnaryOperator<Mono<U>> codeToRun) {
        return new SimpleInstrumentationContext<>(codeToRun, null);
    }

    /**
     * Allows for the more fluent away to return an instrumentation context that runs the specified
     * code on instrumentation step completion.
     *
     * @param codeToRun the code to run on completion
     * @param <U>       the generic type
     *
     * @return an instrumentation context
     */
    public static <U> SimpleInstrumentationContext<U> whenCompleted(BiConsumer<U, Throwable> codeToRun) {
        return new SimpleInstrumentationContext<>(null, codeToRun);
    }
}
