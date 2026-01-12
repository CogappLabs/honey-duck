"use client"

import { useQuery } from "urql"
import Link from "next/link"
import { ASSETS_QUERY, RUNS_QUERY } from "@/lib/queries"

type Asset = {
	id: string
	key: { path: string[] }
	definition?: {
		groupName?: string
		description?: string
		computeKind?: string
	}
}

type Run = {
	runId: string
	jobName: string
	status: string
	startTime: number | null
}

export default function Home() {
	const [assetsResult] = useQuery({ query: ASSETS_QUERY })
	const [runsResult] = useQuery({ query: RUNS_QUERY, variables: { limit: 5 } })

	const assets: Asset[] =
		assetsResult.data?.assetsOrError?.__typename === "AssetConnection"
			? assetsResult.data.assetsOrError.nodes
			: []

	const runs: Run[] =
		runsResult.data?.runsOrError?.__typename === "Runs" ? runsResult.data.runsOrError.results : []

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

	return (
		<div className="space-y-8">
			<div>
				<h1 className="text-3xl font-bold text-gray-900">Honey Duck Dashboard</h1>
				<p className="mt-2 text-gray-600">Pipeline overview and monitoring</p>
			</div>

			{/* Stats */}
			<div className="grid grid-cols-1 md:grid-cols-3 gap-6">
				<div className="bg-white rounded-lg shadow p-6">
					<div className="text-sm font-medium text-gray-500">Total Assets</div>
					<div className="mt-2 text-3xl font-bold text-gray-900">
						{assetsResult.fetching ? "..." : assets.length}
					</div>
				</div>
				<div className="bg-white rounded-lg shadow p-6">
					<div className="text-sm font-medium text-gray-500">Asset Groups</div>
					<div className="mt-2 text-3xl font-bold text-gray-900">
						{assetsResult.fetching ? "..." : Object.keys(assetsByGroup).length}
					</div>
				</div>
				<div className="bg-white rounded-lg shadow p-6">
					<div className="text-sm font-medium text-gray-500">Recent Runs</div>
					<div className="mt-2 text-3xl font-bold text-gray-900">
						{runsResult.fetching ? "..." : runs.length}
					</div>
				</div>
			</div>

			{/* Asset Groups */}
			<div>
				<h2 className="text-xl font-semibold text-gray-900 mb-4">Asset Groups</h2>
				{assetsResult.fetching ? (
					<div className="text-gray-500">Loading assets...</div>
				) : assetsResult.error ? (
					<div className="text-red-600">
						Error loading assets. Is Dagster running at 127.0.0.1:3000?
					</div>
				) : (
					<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
						{Object.entries(assetsByGroup).map(([group, groupAssets]) => (
							<Link
								key={group}
								href={`/assets?group=${group}`}
								className="bg-white rounded-lg shadow p-4 hover:shadow-md transition-shadow"
							>
								<div className="font-medium text-gray-900">{group}</div>
								<div className="text-sm text-gray-500">{groupAssets.length} assets</div>
							</Link>
						))}
					</div>
				)}
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
								</tr>
							</thead>
							<tbody className="bg-white divide-y divide-gray-200">
								{runs.map((run) => (
									<tr key={run.runId} className="hover:bg-gray-50">
										<td className="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-900">
											{run.runId.slice(0, 8)}
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

function StatusBadge({ status }: { status: string }) {
	const colors: Record<string, string> = {
		SUCCESS: "bg-green-100 text-green-800",
		FAILURE: "bg-red-100 text-red-800",
		STARTED: "bg-blue-100 text-blue-800",
		QUEUED: "bg-yellow-100 text-yellow-800",
		CANCELED: "bg-gray-100 text-gray-800",
	}

	return (
		<span
			className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${colors[status] || "bg-gray-100 text-gray-800"}`}
		>
			{status}
		</span>
	)
}
