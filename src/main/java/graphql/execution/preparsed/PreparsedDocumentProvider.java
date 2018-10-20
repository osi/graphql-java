package graphql.execution.preparsed;


import reactor.core.publisher.Mono;

/**
 * Interface that allows clients to hook in Document caching and/or the whitelisting of queries
 */
@FunctionalInterface
public interface PreparsedDocumentProvider {
    /**
     * This is called to get a "cached" pre-parsed query and if its not present, then the computeFunction
     * can be called to parse the query
     *
     * @param query           The graphql query
     * @param compute If the query has not be pre-parsed, this function can be called to parse it
     *
     * @return an instance of {@link PreparsedDocumentEntry}
     */
    Mono<PreparsedDocumentEntry> get(String query, Mono<PreparsedDocumentEntry> compute);

    /**
     * Return the canonical form of the query
     *
     * @param query The graphql query
     *
     * @return The canonical form of the query
     */
    default String canonicalize(String query) {
        return query;
    }
}


