"use client"

import { useQuery } from "urql"
import Link from "next/link"
import { ASSET_DETAILS_QUERY } from "@/lib/queries"
import { formatDuration } from "@/lib/utils"

type Props = {
	params: Promise<{ path: string[] }>
}

const AssetDetailPage = ({ params }: Props) => {
	const { path } = require("react").use(params)
	const assetKey = { path }

	const [result] = useQuery({
		query: ASSET_DETAILS_QUERY,
		variables: { assetKey },
	})

	const asset = result.data?.assetOrError?.__typename === "Asset" ? result.data.assetOrError : null
	const notFound = result.data?.assetOrError?.__typename === "AssetNotFoundError"

	return (
		<div className="space-y-6">
			<div className="flex items-center gap-2 text-sm text-gray-500">
				<Link href="/assets" className="hover:text-amber-600">
					Assets
				</Link>
				<span>/</span>
				<span className="text-gray-900">{path.join(" / ")}</span>
			</div>

			{result.fetching ? (
				<div className="text-gray-500">Loading asset...</div>
			) : result.error ? (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<p className="text-red-600">Error loading asset. Is Dagster running at 127.0.0.1:3000?</p>
					<p className="text-sm text-red-500 mt-1">{result.error.message}</p>
				</div>
			) : notFound ? (
				<div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
					<p className="text-yellow-800">Asset not found: {path.join("/")}</p>
				</div>
			) : asset ? (
				<>
					<div className="bg-white rounded-lg shadow p-6">
						<h1 className="text-2xl font-bold text-gray-900">{path.join(" / ")}</h1>
						{asset.definition?.groupName && (
							<Link
								href={`/assets?group=${asset.definition.groupName}`}
								className="inline-block mt-2 text-sm text-amber-600 hover:text-amber-700"
							>
								{asset.definition.groupName}
							</Link>
						)}
						{asset.definition?.description && (
							<p className="mt-4 text-gray-600">{asset.definition.description}</p>
						)}
						{asset.definition?.computeKind && (
							<div className="mt-4">
								<span className="px-2 py-1 text-xs bg-gray-100 rounded">
									{asset.definition.computeKind}
								</span>
							</div>
						)}
					</div>

					{asset.assetMaterializations && asset.assetMaterializations.length > 0 && (
						<div className="bg-white rounded-lg shadow overflow-hidden">
							<div className="px-6 py-4 border-b border-gray-200">
								<h2 className="text-lg font-semibold text-gray-900">Recent Materializations</h2>
							</div>
							<table className="min-w-full divide-y divide-gray-200">
								<thead className="bg-gray-50">
									<tr>
										<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
											Run ID
										</th>
										<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
											Timestamp
										</th>
										<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
											Metadata
										</th>
									</tr>
								</thead>
								<tbody className="bg-white divide-y divide-gray-200">
									{asset.assetMaterializations.map(
										(mat: { runId: string; timestamp: string; metadataEntries: { label: string; text?: string; intValue?: number; floatValue?: number }[] }, i: number) => (
											<tr key={i} className="hover:bg-gray-50">
												<td className="px-6 py-4 whitespace-nowrap text-sm font-mono">
													<a
														href={`http://127.0.0.1:3000/runs/${mat.runId}`}
														target="_blank"
														rel="noopener noreferrer"
														className="text-amber-600 hover:text-amber-700"
													>
														{mat.runId.slice(0, 8)}
													</a>
												</td>
												<td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
													{new Date(Number(mat.timestamp)).toLocaleString()}
												</td>
												<td className="px-6 py-4 text-sm text-gray-500">
													<div className="flex flex-wrap gap-2">
														{mat.metadataEntries.slice(0, 3).map((entry, j: number) => (
															<span key={j} className="px-2 py-1 text-xs bg-gray-100 rounded">
																{entry.label}: {entry.text ?? entry.intValue ?? entry.floatValue ?? "..."}
															</span>
														))}
														{mat.metadataEntries.length > 3 && (
															<span className="text-xs text-gray-400">
																+{mat.metadataEntries.length - 3} more
															</span>
														)}
													</div>
												</td>
											</tr>
										)
									)}
								</tbody>
							</table>
						</div>
					)}
				</>
			) : null}
		</div>
	)
}

export default AssetDetailPage
