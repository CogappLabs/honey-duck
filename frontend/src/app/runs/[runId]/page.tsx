"use client"

import { useQuery } from "urql"
import Link from "next/link"
import { use } from "react"
import { RUN_WITH_ASSETS_QUERY, ASSET_DEPENDENCIES_QUERY } from "@/lib/queries"
import { DAGSTER_URL } from "@/lib/config"
import { StatusBadge } from "@/components/StatusBadge"
import { FailedRunError } from "@/components/FailedRunError"
import { formatDuration, formatDateTime, formatRelativeTime } from "@/lib/utils"

type AssetMaterialization = {
	assetKey: { path: string[] }
	timestamp: string
}

type Props = {
	params: Promise<{ runId: string }>
}

const AssetWithDeps = ({ assetKey, allAssets }: { assetKey: string[]; allAssets: Set<string> }) => {
	const [result] = useQuery({
		query: ASSET_DEPENDENCIES_QUERY,
		variables: { assetKey: { path: assetKey } },
	})

	const asset = result.data?.assetOrError
	const deps = asset?.definition?.dependencyKeys || []
	const dependedBy = asset?.definition?.dependedByKeys || []

	// Filter to only show deps/dependedBy that are in this run
	const depsInRun = deps.filter((d: { path: string[] }) => allAssets.has(d.path.join("/")))
	const dependedByInRun = dependedBy.filter((d: { path: string[] }) => allAssets.has(d.path.join("/")))

	return (
		<div className="bg-white rounded-lg shadow p-4">
			<Link
				href={`/assets/${assetKey.join("/")}`}
				className="font-medium text-amber-600 hover:text-amber-700"
			>
				{assetKey.join(" / ")}
			</Link>
			{(depsInRun.length > 0 || dependedByInRun.length > 0) && (
				<div className="mt-2 text-sm text-gray-500">
					{depsInRun.length > 0 && (
						<div>
							<span className="text-gray-400">depends on: </span>
							{depsInRun.map((d: { path: string[] }, i: number) => (
								<span key={d.path.join("/")}>
									{i > 0 && ", "}
									<Link
										href={`/assets/${d.path.join("/")}`}
										className="text-gray-600 hover:text-amber-600"
									>
										{d.path.join("/")}
									</Link>
								</span>
							))}
						</div>
					)}
					{dependedByInRun.length > 0 && (
						<div>
							<span className="text-gray-400">used by: </span>
							{dependedByInRun.map((d: { path: string[] }, i: number) => (
								<span key={d.path.join("/")}>
									{i > 0 && ", "}
									<Link
										href={`/assets/${d.path.join("/")}`}
										className="text-gray-600 hover:text-amber-600"
									>
										{d.path.join("/")}
									</Link>
								</span>
							))}
						</div>
					)}
				</div>
			)}
		</div>
	)
}

const RunDetailPage = ({ params }: Props) => {
	const { runId } = use(params)

	const [result] = useQuery({
		query: RUN_WITH_ASSETS_QUERY,
		variables: { runId },
	})

	const run = result.data?.runOrError?.__typename === "Run" ? result.data.runOrError : null
	const notFound = result.data?.runOrError?.__typename === "RunNotFoundError"

	const materializations: AssetMaterialization[] = run?.assetMaterializations || []
	const allAssets = new Set(materializations.map((m) => m.assetKey.path.join("/")))

	// Sort by timestamp
	const sortedMaterializations = [...materializations].sort(
		(a, b) => Number(a.timestamp) - Number(b.timestamp)
	)

	return (
		<div className="space-y-6">
			<div className="flex items-center gap-2 text-sm text-gray-500">
				<Link href="/runs" className="hover:text-amber-600">
					Runs
				</Link>
				<span>/</span>
				<span className="text-gray-900 font-mono">{runId.slice(0, 8)}</span>
			</div>

			{result.fetching ? (
				<div className="text-gray-500">Loading run...</div>
			) : result.error ? (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<p className="text-red-600">Error loading run. Is Dagster running?</p>
					<p className="text-sm text-red-500 mt-1">{result.error.message}</p>
				</div>
			) : notFound ? (
				<div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
					<p className="text-yellow-800">Run not found: {runId}</p>
				</div>
			) : run ? (
				<>
					<div className="bg-white rounded-lg shadow p-6">
						<div className="flex items-center justify-between">
							<div>
								<h1 className="text-2xl font-bold text-gray-900">{run.jobName}</h1>
								<p className="mt-1 text-sm text-gray-500 font-mono">{runId}</p>
							</div>
							<div className="flex items-center gap-4">
								<StatusBadge status={run.status} />
								<a
									href={`${DAGSTER_URL}/runs/${runId}`}
									target="_blank"
									rel="noopener noreferrer"
									className="text-sm text-amber-600 hover:text-amber-700"
								>
									View in Dagster
								</a>
							</div>
						</div>
						<div className="mt-4 flex flex-wrap gap-6 text-sm text-gray-600">
							{run.startTime && (
								<span title={formatDateTime(run.startTime)}>
									{formatRelativeTime(run.startTime)}
								</span>
							)}
							{run.startTime && run.endTime && (
								<span>
									{formatDuration(run.endTime - run.startTime)}
								</span>
							)}
							{run.startTime && !run.endTime && (
								<span className="text-blue-600">Running...</span>
							)}
							<span>{materializations.length} assets</span>
						</div>
						{run.status === "FAILURE" && (
							<div className="mt-4">
								<FailedRunError runId={runId} />
							</div>
						)}
					</div>

					{sortedMaterializations.length > 0 && (
						<div>
							<h2 className="text-xl font-semibold text-gray-900 mb-4">
								Materialized Assets ({sortedMaterializations.length})
							</h2>
							<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
								{sortedMaterializations.map((mat) => (
									<AssetWithDeps
										key={mat.assetKey.path.join("/")}
										assetKey={mat.assetKey.path}
										allAssets={allAssets}
									/>
								))}
							</div>
						</div>
					)}
				</>
			) : null}
		</div>
	)
}

export default RunDetailPage
