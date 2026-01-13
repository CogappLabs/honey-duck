"use client"

import { useQuery } from "urql"
import Link from "next/link"
import { ASSET_DETAILS_QUERY } from "@/lib/queries"
import { DAGSTER_URL } from "@/lib/config"

type MetadataEntry = {
	label: string
	text?: string
	intValue?: number
	floatValue?: number
}

const formatValue = (entry: MetadataEntry): string => {
	if (entry.text !== undefined) return entry.text
	if (entry.intValue !== undefined) return entry.intValue.toLocaleString()
	if (entry.floatValue !== undefined) return entry.floatValue.toLocaleString()
	return "-"
}

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
					<p className="text-red-600">Error loading asset. Is Dagster running?</p>
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
							<div className="divide-y divide-gray-200">
								{asset.assetMaterializations.map(
									(mat: { runId: string; timestamp: string; metadataEntries: MetadataEntry[] }, i: number) => {
										const metadata = mat.metadataEntries || []
										return (
											<div key={i} className="p-6">
												<div className="flex items-center gap-4 mb-3">
													<a
														href={`${DAGSTER_URL}/runs/${mat.runId}`}
														target="_blank"
														rel="noopener noreferrer"
														className="text-sm font-mono text-amber-600 hover:text-amber-700"
													>
														{mat.runId.slice(0, 8)}
													</a>
													<span className="text-sm text-gray-500">
														{new Date(Number(mat.timestamp)).toLocaleString()}
													</span>
												</div>
												{metadata.length > 0 && (
													<details className="text-sm">
														<summary className="cursor-pointer text-gray-500 hover:text-gray-700">
															{metadata.length} metadata {metadata.length === 1 ? "entry" : "entries"}
														</summary>
														<div className="mt-3 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
															{metadata.map((entry, j) => (
																<div key={j} className="bg-gray-50 rounded p-3">
																	<div className="text-xs font-medium text-gray-500 mb-1">
																		{entry.label}
																	</div>
																	<div className="text-sm text-gray-900 break-words">
																		{formatValue(entry)}
																	</div>
																</div>
															))}
														</div>
													</details>
												)}
											</div>
										)
									}
								)}
							</div>
						</div>
					)}
				</>
			) : null}
		</div>
	)
}

export default AssetDetailPage
