"use client"

import { useQuery } from "urql"
import Link from "next/link"
import { ASSETS_QUERY, RUNS_QUERY } from "@/lib/queries"
import { DAGSTER_URL } from "@/lib/config"
import { StatusBadge } from "@/components/StatusBadge"
import { FailedRunError } from "@/components/FailedRunError"
import { formatDuration } from "@/lib/utils"
import type { Asset, Run } from "@/lib/types"

const Home = () => {
	const [assetsResult] = useQuery({ query: ASSETS_QUERY })
	const [runsResult] = useQuery({ query: RUNS_QUERY, variables: { limit: 50 } })

	const assets: Asset[] =
		assetsResult.data?.assetsOrError?.__typename === "AssetConnection"
			? assetsResult.data.assetsOrError.nodes
			: []

	const allRuns: Run[] =
		runsResult.data?.runsOrError?.__typename === "Runs" ? runsResult.data.runsOrError.results : []

	// Filter runs from last 24 hours
	const oneDayAgo = Date.now() / 1000 - 24 * 60 * 60
	const recentRuns = allRuns.filter((run) => run.startTime && run.startTime > oneDayAgo)

	// Calculate run stats
	const successCount = recentRuns.filter((run) => run.status === "SUCCESS").length
	const failedCount = recentRuns.filter((run) => run.status === "FAILURE").length
	const runningCount = recentRuns.filter((run) => run.status === "STARTED").length

	// Get recent failures for alert
	const recentFailures = allRuns.filter((run) => run.status === "FAILURE").slice(0, 3)

	// Get last successful run
	const lastSuccess = allRuns.find((run) => run.status === "SUCCESS")
	const lastSuccessTime = lastSuccess?.startTime
		? new Date(lastSuccess.startTime * 1000)
		: null

	// Recent runs for table (limit to 5)
	const runs = allRuns.slice(0, 5)

	// Group assets by group name
	const assetsByGroup = assets.reduce(
		(acc: Record<string, Asset[]>, asset: Asset) => {
			const group = asset.definition?.groupName || "ungrouped"
			if (!acc[group]) acc[group] = []
			acc[group].push(asset)
			return acc
		},
		{} as Record<string, Asset[]>
	)

	// Determine overall health
	const isHealthy = failedCount === 0 && recentRuns.length > 0
	const hasWarning = failedCount > 0 && successCount > failedCount
	const hasCritical = failedCount > 0 && failedCount >= successCount

	return (
		<div className="space-y-8">
			<div className="flex justify-between items-start">
				<div>
					<h1 className="text-3xl font-bold text-gray-900">Honey Duck Dashboard</h1>
					<p className="mt-2 text-gray-600">Pipeline overview and monitoring</p>
				</div>
				{lastSuccessTime && (
					<div className="text-right text-sm text-gray-500">
						<div>Last successful run</div>
						<div className="font-medium text-gray-700">{lastSuccessTime.toLocaleString()}</div>
					</div>
				)}
			</div>

			{/* Failures Alert */}
			{recentFailures.length > 0 && (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<div className="flex items-center gap-2 mb-3">
						<span className="text-red-600 font-semibold">Recent Failures</span>
					</div>
					<div className="space-y-3">
						{recentFailures.map((run) => (
							<div key={run.runId} className="text-sm">
								<div className="flex justify-between items-center">
									<span className="font-medium text-gray-900">{run.jobName}</span>
									<div className="flex items-center gap-4">
										<span className="text-gray-500">
											{run.startTime ? new Date(run.startTime * 1000).toLocaleString() : "-"}
										</span>
										<Link
											href={`/runs/${run.runId}`}
											className="text-amber-600 hover:text-amber-700"
										>
											View
										</Link>
									</div>
								</div>
								<FailedRunError runId={run.runId} />
							</div>
						))}
					</div>
				</div>
			)}

			{/* Pipeline Health Stats */}
			<div className="grid grid-cols-1 md:grid-cols-4 gap-4">
				<div className={`rounded-lg shadow p-6 ${isHealthy ? "bg-green-50 border border-green-200" : hasCritical ? "bg-red-50 border border-red-200" : hasWarning ? "bg-yellow-50 border border-yellow-200" : "bg-white"}`}>
					<div className="text-sm font-medium text-gray-500">Pipeline Health</div>
					<div className={`mt-2 text-2xl font-bold ${isHealthy ? "text-green-700" : hasCritical ? "text-red-700" : hasWarning ? "text-yellow-700" : "text-gray-400"}`}>
						{runsResult.fetching ? "..." : isHealthy ? "Healthy" : hasCritical ? "Critical" : hasWarning ? "Warning" : "No Data"}
					</div>
				</div>
				<div className="bg-white rounded-lg shadow p-6">
					<div className="text-sm font-medium text-gray-500">Successful (24h)</div>
					<div className="mt-2 text-2xl font-bold text-green-600">
						{runsResult.fetching ? "..." : successCount}
					</div>
				</div>
				<div className="bg-white rounded-lg shadow p-6">
					<div className="text-sm font-medium text-gray-500">Failed (24h)</div>
					<div className="mt-2 text-2xl font-bold text-red-600">
						{runsResult.fetching ? "..." : failedCount}
					</div>
				</div>
				<div className="bg-white rounded-lg shadow p-6">
					<div className="text-sm font-medium text-gray-500">Running</div>
					<div className="mt-2 text-2xl font-bold text-blue-600">
						{runsResult.fetching ? "..." : runningCount}
					</div>
				</div>
			</div>

			{/* Total Assets */}
			<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
				<Link href="/assets" className="bg-white rounded-lg shadow p-6 hover:shadow-md transition-shadow">
					<div className="text-sm font-medium text-gray-500">Total Assets</div>
					<div className="mt-2 text-2xl font-bold text-gray-900">
						{assetsResult.fetching ? "..." : assets.length}
					</div>
					<div className="mt-1 text-sm text-amber-600">View all assets →</div>
				</Link>
				<Link href="/jobs" className="bg-white rounded-lg shadow p-6 hover:shadow-md transition-shadow">
					<div className="text-sm font-medium text-gray-500">Asset Groups</div>
					<div className="mt-2 text-2xl font-bold text-gray-900">
						{assetsResult.fetching ? "..." : Object.keys(assetsByGroup).length}
					</div>
					<div className="mt-1 text-sm text-amber-600">View jobs →</div>
				</Link>
			</div>

			{/* Recent Runs */}
			<div>
				<div className="flex justify-between items-center mb-4">
					<h2 className="text-xl font-semibold text-gray-900">Recent Runs</h2>
					<Link href="/runs" className="text-amber-600 hover:text-amber-700 text-sm">
						View all
					</Link>
				</div>
				{runsResult.fetching ? (
					<div className="text-gray-500">Loading runs...</div>
				) : runsResult.error ? (
					<div className="text-red-600">Error loading runs. Is Dagster running?</div>
				) : runs.length === 0 ? (
					<div className="text-gray-500">No runs yet</div>
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
									</tr>
								))}
							</tbody>
						</table>
					</div>
				)}
			</div>
		</div>
	)
}

export default Home
