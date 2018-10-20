package graphql.execution.pubsub;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

/**
 * This example publisher will create count "messages" and then terminate.
 */
public class MessagePublisher implements Publisher<Message> {

    private final Flux<Message> flux;

    public MessagePublisher(int count) {
        flux = Flux.range(0, count)
                   .map(at -> examineMessage(new Message("sender" + at, "text" + at), at));
    }

    @Override
    public void subscribe(Subscriber<? super Message> s) {
        flux.subscribe(s);
    }

    @SuppressWarnings("unused")
    protected Message examineMessage(Message message, Integer at) {
        return message;
    }
}
