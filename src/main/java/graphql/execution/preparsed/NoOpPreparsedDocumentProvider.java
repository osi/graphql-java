package graphql.execution.preparsed;


import reactor.core.publisher.Mono;

public class NoOpPreparsedDocumentProvider implements PreparsedDocumentProvider {
    public static final NoOpPreparsedDocumentProvider INSTANCE = new NoOpPreparsedDocumentProvider();

    @Override
    public Mono<PreparsedDocumentEntry> get(String query, Mono<PreparsedDocumentEntry> compute) {
        return compute;
    }
}
