"use client"

import { useQuery } from "urql"
import { RUN_ERROR_QUERY } from "@/lib/queries"

type Event = {
	message?: string
	error?: {
		message: string
	} | null
}

export const FailedRunError = ({ runId }: { runId: string }) => {
	const [result] = useQuery({
		query: RUN_ERROR_QUERY,
		variables: { runId },
	})

	if (result.fetching) {
		return (
			<details className="mt-2">
				<summary className="cursor-pointer text-red-600 hover:text-red-700">
					Loading error...
				</summary>
			</details>
		)
	}

	const events: Event[] = result.data?.runOrError?.eventConnection?.events || []

	// Find the error message from events
	const errorEvent = events.find((e) => e.error?.message)
	const errorMessage = errorEvent?.error?.message

	if (!errorMessage) {
		return null
	}

	return (
		<details className="mt-2">
			<summary className="cursor-pointer text-red-600 hover:text-red-700">
				Error details
			</summary>
			<pre className="mt-2 p-3 bg-red-100 rounded text-xs text-red-800 overflow-x-auto whitespace-pre-wrap">
				{errorMessage}
			</pre>
		</details>
	)
}
