import { NextRequest, NextResponse } from "next/server"
import { DAGSTER_URL } from "@/lib/config"

const DAGSTER_GRAPHQL_URL = process.env.DAGSTER_GRAPHQL_URL || `${DAGSTER_URL}/graphql`

export async function POST(request: NextRequest) {
	try {
		const body = await request.json()

		const response = await fetch(DAGSTER_GRAPHQL_URL, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify(body),
		})

		const data = await response.json()
		return NextResponse.json(data)
	} catch (error) {
		console.error("GraphQL proxy error:", error)
		return NextResponse.json(
			{ errors: [{ message: "Failed to connect to Dagster GraphQL API" }] },
			{ status: 502 }
		)
	}
}

export async function GET(request: NextRequest) {
	try {
		const { searchParams } = new URL(request.url)
		const query = searchParams.get("query")
		const variables = searchParams.get("variables")
		const operationName = searchParams.get("operationName")

		const params = new URLSearchParams()
		if (query) params.set("query", query)
		if (variables) params.set("variables", variables)
		if (operationName) params.set("operationName", operationName)

		const response = await fetch(`${DAGSTER_GRAPHQL_URL}?${params.toString()}`, {
			method: "GET",
			headers: {
				"Content-Type": "application/json",
			},
		})

		const data = await response.json()
		return NextResponse.json(data)
	} catch (error) {
		console.error("GraphQL proxy error:", error)
		return NextResponse.json(
			{ errors: [{ message: "Failed to connect to Dagster GraphQL API" }] },
			{ status: 502 }
		)
	}
}
