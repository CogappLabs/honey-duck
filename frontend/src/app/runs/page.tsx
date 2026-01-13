"use client"

import { useQuery } from "urql"
import Link from "next/link"
import { RUNS_QUERY } from "@/lib/queries"
import { DAGSTER_URL } from "@/lib/config"
import { StatusBadge } from "@/components/StatusBadge"
import { formatDuration } from "@/lib/utils"
import type { Run } from "@/lib/types"

const RunsPage = () => {
	const [result] = useQuery({ query: RUNS_QUERY, variables: { limit: 50 } })

	const runs: Run[] =
		result.data?.runsOrError?.__typename === "Runs" ? result.data.runsOrError.results : []

	return (
		<div className="space-y-6">
			<div>
				<h1 className="text-3xl font-bold text-gray-900">Runs</h1>
				<p className="mt-1 text-gray-600">Pipeline execution history</p>
			</div>

			{result.fetching ? (
				<div className="text-gray-500">Loading runs...</div>
			) : result.error ? (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<p className="text-red-600">Error loading runs. Is Dagster running?</p>
					<p className="text-sm text-red-500 mt-1">{result.error.message}</p>
				</div>
			) : runs.length === 0 ? (
				<div className="bg-white rounded-lg shadow p-8 text-center text-gray-500">
					No runs yet. Run a job from the Dagster UI to see results here.
				</div>
			) : (
				<div className="bg-white rounded-lg shadow overflow-hidden">
					<table className="min-w-full divide-y divide-gray-200">
						<thead className="bg-gray-50">
							<tr>
								<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
									Run ID
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
									Job
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
									Status
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
									Started
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
									Duration
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
									Actions
								</th>
							</tr>
						</thead>
						<tbody className="bg-white divide-y divide-gray-200">
							{runs.map((run) => (
								<tr key={run.runId} className="hover:bg-gray-50">
										<td className="px-6 py-4 whitespace-nowrap text-sm font-mono">
											<Link
												href={`/runs/${run.runId}`}
												className="text-amber-600 hover:text-amber-700"
											>
												{run.runId.slice(0, 8)}
											</Link>
										</td>
										<td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
											{run.jobName}
										</td>
										<td className="px-6 py-4 whitespace-nowrap">
											<StatusBadge status={run.status} />
										</td>
										<td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
											{run.startTime ? new Date(run.startTime * 1000).toLocaleString() : "-"}
										</td>
										<td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
											{run.startTime && run.endTime
												? formatDuration(run.endTime - run.startTime)
												: run.startTime
													? "Running..."
													: "-"}
										</td>
										<td className="px-6 py-4 whitespace-nowrap text-sm">
											<a
												href={`${DAGSTER_URL}/runs/${run.runId}`}
												target="_blank"
												rel="noopener noreferrer"
												className="text-amber-600 hover:text-amber-700"
											>
												View in Dagster
											</a>
										</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			)}
		</div>
	)
}

export default RunsPage
