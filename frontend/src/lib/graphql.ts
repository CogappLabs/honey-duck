import { cacheExchange, Client, fetchExchange } from "urql"

// Use local API route to proxy requests (avoids CORS issues)
const DAGSTER_GRAPHQL_URL = "/api/graphql"

export function createClient() {
	return new Client({
		url: DAGSTER_GRAPHQL_URL,
		exchanges: [cacheExchange, fetchExchange],
	})
}
