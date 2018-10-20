package graphql.execution.preparsed

import reactor.core.publisher.Mono

class TestingPreparsedDocumentProvider implements PreparsedDocumentProvider {
    private Map<String, Mono<PreparsedDocumentEntry>> cache = new HashMap<>()

    @Override
    Mono<PreparsedDocumentEntry> get(String query, Mono<PreparsedDocumentEntry> compute) {
        return cache.computeIfAbsent(query, { q -> compute.cache() })
    }

}
