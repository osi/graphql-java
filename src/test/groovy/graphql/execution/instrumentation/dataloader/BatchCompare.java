package graphql.execution.instrumentation.dataloader;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static java.nio.charset.Charset.defaultCharset;

public class BatchCompare {

    GraphQLSchema buildDataLoaderSchema() {
        InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("storesanddepartments.graphqls");
        Reader streamReader = new InputStreamReader(resourceAsStream, defaultCharset());
        TypeDefinitionRegistry typeDefinitionRegistry = new SchemaParser().parse(streamReader);
        RuntimeWiring runtimeWiring = RuntimeWiring.newRuntimeWiring()
                .type(TypeRuntimeWiring.newTypeWiring("Query")
                        .dataFetcher("shops", BatchCompareDataFetchers.shopsDataFetcher)
                        .dataFetcher("expensiveShops", BatchCompareDataFetchers.expensiveShopsDataFetcher)
                )
                .type(TypeRuntimeWiring.newTypeWiring("Shop")
                        .dataFetcher("departments", BatchCompareDataFetchers.departmentsForShopDataLoaderDataFetcher)
                        .dataFetcher("expensiveDepartments", BatchCompareDataFetchers.departmentsForShopDataLoaderDataFetcher)
                )
                .type(TypeRuntimeWiring.newTypeWiring("Department")
                        .dataFetcher("products", BatchCompareDataFetchers.productsForDepartmentDataLoaderDataFetcher)
                        .dataFetcher("expensiveProducts", BatchCompareDataFetchers.productsForDepartmentDataLoaderDataFetcher)
                )
                .build();

        return new SchemaGenerator().makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);
    }
}

