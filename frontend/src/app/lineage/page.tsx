"use client"

import { useQuery } from "urql"
import { useState } from "react"
import { ALL_LINEAGE_QUERY } from "@/lib/queries"

interface ColumnDep {
	assetKey: { path: string[] }
	columnName: string
}

interface ColumnLineage {
	columnName: string
	columnDeps: ColumnDep[]
}

interface MetadataEntry {
	label: string
	lineage?: ColumnLineage[]
	jsonString?: string
}

interface AssetNode {
	key: { path: string[] }
	definition?: { groupName?: string }
	assetMaterializations: Array<{
		metadataEntries: MetadataEntry[]
	}>
}

interface LineageExamples {
	[column: string]: string | null
}

const LineagePage = () => {
	const [result] = useQuery({ query: ALL_LINEAGE_QUERY })
	const [selectedAsset, setSelectedAsset] = useState<string | null>(null)
	const [selectedColumn, setSelectedColumn] = useState<string | null>(null)

	const assets: AssetNode[] =
		result.data?.assetsOrError?.__typename === "AssetConnection"
			? result.data.assetsOrError.nodes
			: []

	// Filter assets that have column lineage
	const assetsWithLineage = assets.filter((asset) => {
		const materialization = asset.assetMaterializations?.[0]
		const lineageEntry = materialization?.metadataEntries?.find(
			(e) => e.label === "dagster/column_lineage"
		)
		return lineageEntry?.lineage && lineageEntry.lineage.length > 0
	})

	// Build examples map for ALL assets (including sources without lineage)
	const allExamplesMap = new Map<string, LineageExamples>()
	for (const asset of assets) {
		const assetKey = asset.key.path.join("/")
		const entries = asset.assetMaterializations?.[0]?.metadataEntries
		const examplesEntry = entries?.find((e) => e.label === "lineage_examples")
		if (examplesEntry?.jsonString) {
			try {
				allExamplesMap.set(assetKey, JSON.parse(examplesEntry.jsonString))
			} catch {
				// ignore parse errors
			}
		}
	}

	// Build lineage map with examples (only for assets with lineage)
	const lineageMap = new Map<
		string,
		{ asset: AssetNode; lineage: ColumnLineage[]; examples: LineageExamples }
	>()
	for (const asset of assetsWithLineage) {
		const assetKey = asset.key.path.join("/")
		const entries = asset.assetMaterializations[0]?.metadataEntries
		const lineageEntry = entries?.find((e) => e.label === "dagster/column_lineage")
		const examples = allExamplesMap.get(assetKey) || {}

		if (lineageEntry?.lineage) {
			lineageMap.set(assetKey, { asset, lineage: lineageEntry.lineage, examples })
		}
	}

	// Find all columns that depend on a given source column
	const findDependents = (sourceAsset: string, sourceColumn: string) => {
		const dependents: Array<{ asset: string; column: string; example?: string }> = []
		for (const [assetKey, { lineage, examples }] of lineageMap.entries()) {
			for (const col of lineage) {
				for (const dep of col.columnDeps) {
					if (
						dep.assetKey.path.join("/") === sourceAsset &&
						dep.columnName === sourceColumn
					) {
						dependents.push({
							asset: assetKey,
							column: col.columnName,
							example: examples[col.columnName] ?? undefined
						})
					}
				}
			}
		}
		return dependents
	}

	// Get example value for a source column (works for all assets, including harvest)
	const getSourceExample = (sourceAsset: string, sourceColumn: string): string | undefined => {
		const examples = allExamplesMap.get(sourceAsset)
		return examples?.[sourceColumn] ?? undefined
	}

	const selectedLineage = selectedAsset ? lineageMap.get(selectedAsset) : null

	return (
		<div className="space-y-6">
			<div>
				<h1 className="text-3xl font-bold text-gray-900">Column Lineage</h1>
				<p className="mt-1 text-gray-600">
					Track how columns flow through your pipeline with example values
				</p>
			</div>

			{result.fetching ? (
				<div className="text-gray-500">Loading lineage data...</div>
			) : result.error ? (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<p className="text-red-600">
						Error loading lineage. Is Dagster running?
					</p>
					<p className="text-sm text-red-500 mt-1">{result.error.message}</p>
				</div>
			) : assetsWithLineage.length === 0 ? (
				<div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
					<p className="text-amber-700">
						No column lineage found. Run the duckdb_soda_pipeline to generate
						lineage metadata.
					</p>
				</div>
			) : (
				<div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
					{/* Asset selector */}
					<div className="bg-white rounded-lg shadow p-4">
						<h2 className="text-lg font-semibold text-gray-900 mb-4">
							Assets with Lineage
						</h2>
						<div className="space-y-2">
							{Array.from(lineageMap.keys()).map((assetKey) => (
								<button
									key={assetKey}
									onClick={() => {
										setSelectedAsset(assetKey)
										setSelectedColumn(null)
									}}
									className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors ${
										selectedAsset === assetKey
											? "bg-amber-100 text-amber-800 font-medium"
											: "hover:bg-gray-100 text-gray-700"
									}`}
								>
									{assetKey}
								</button>
							))}
						</div>
					</div>

					{/* Column lineage table */}
					<div className="lg:col-span-2 bg-white rounded-lg shadow">
						{selectedLineage ? (
							<div>
								<div className="px-4 py-3 border-b border-gray-200">
									<h2 className="text-lg font-semibold text-gray-900">
										{selectedAsset}
									</h2>
									<p className="text-sm text-gray-500">
										{selectedLineage.lineage.length} columns tracked
									</p>
								</div>
								<div className="overflow-x-auto">
									<table className="min-w-full divide-y divide-gray-200">
										<thead className="bg-gray-50">
											<tr>
												<th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
													Column
												</th>
												<th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
													Example
												</th>
												<th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
													Source(s)
												</th>
											</tr>
										</thead>
										<tbody className="bg-white divide-y divide-gray-200">
											{selectedLineage.lineage
												.sort((a, b) => a.columnName.localeCompare(b.columnName))
												.map((col) => (
													<tr
														key={col.columnName}
														className={`hover:bg-gray-50 cursor-pointer ${
															selectedColumn === col.columnName
																? "bg-amber-50"
																: ""
														}`}
														onClick={() => setSelectedColumn(col.columnName)}
													>
														<td className="px-4 py-3 whitespace-nowrap">
															<code className="text-sm font-mono text-amber-700">
																{col.columnName}
															</code>
														</td>
														<td className="px-4 py-3 whitespace-nowrap">
															{selectedLineage.examples[col.columnName] && (
																<span className="text-sm text-gray-600 font-mono">
																	{selectedLineage.examples[col.columnName]}
																</span>
															)}
														</td>
														<td className="px-4 py-3">
															<div className="flex flex-wrap gap-2">
																{col.columnDeps.map((dep, i) => (
																	<span
																		key={i}
																		className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-gray-100 text-gray-700"
																	>
																		<span className="text-gray-500">
																			{dep.assetKey.path.join("/")}
																		</span>
																		<span className="mx-1 text-gray-400">.</span>
																		<code className="font-mono font-medium">
																			{dep.columnName}
																		</code>
																	</span>
																))}
															</div>
														</td>
													</tr>
												))}
										</tbody>
									</table>
								</div>
							</div>
						) : (
							<div className="p-8 text-center text-gray-500">
								Select an asset to view its column lineage
							</div>
						)}
					</div>
				</div>
			)}

			{/* Lineage visualization with examples */}
			{selectedAsset && selectedColumn && selectedLineage && (
				<div className="bg-white rounded-lg shadow p-4">
					<h3 className="text-lg font-semibold text-gray-900 mb-4">
						Lineage for{" "}
						<code className="text-amber-700">{selectedColumn}</code>
						{selectedLineage.examples[selectedColumn] && (
							<span className="ml-2 text-base font-normal text-gray-500">
								= <span className="font-mono">{selectedLineage.examples[selectedColumn]}</span>
							</span>
						)}
					</h3>

					{/* Find the selected column's lineage */}
					{(() => {
						const colLineage = selectedLineage.lineage.find(
							(c) => c.columnName === selectedColumn
						)
						if (!colLineage) return null

						return (
							<div className="flex items-center gap-4 overflow-x-auto py-4">
								{/* Sources */}
								<div className="flex flex-col gap-2">
									{colLineage.columnDeps.map((dep, i) => {
										const sourceExample = getSourceExample(
											dep.assetKey.path.join("/"),
											dep.columnName
										)
										return (
											<div
												key={i}
												className="px-3 py-2 bg-blue-50 border border-blue-200 rounded-lg text-sm"
											>
												<div className="text-blue-600 font-medium text-xs">
													{dep.assetKey.path.join("/")}
												</div>
												<code className="text-blue-800">{dep.columnName}</code>
												{sourceExample && (
													<div className="text-blue-600 font-mono text-xs mt-1">
														{sourceExample}
													</div>
												)}
											</div>
										)
									})}
								</div>

								{/* Arrow */}
								<div className="flex-shrink-0 text-gray-400">
									<svg
										className="w-8 h-8"
										fill="none"
										stroke="currentColor"
										viewBox="0 0 24 24"
									>
										<path
											strokeLinecap="round"
											strokeLinejoin="round"
											strokeWidth={2}
											d="M14 5l7 7m0 0l-7 7m7-7H3"
										/>
									</svg>
								</div>

								{/* Target */}
								<div className="px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-sm">
									<div className="text-amber-600 font-medium text-xs">{selectedAsset}</div>
									<code className="text-amber-800">{selectedColumn}</code>
									{selectedLineage.examples[selectedColumn] && (
										<div className="text-amber-600 font-mono text-xs mt-1">
											{selectedLineage.examples[selectedColumn]}
										</div>
									)}
								</div>

								{/* Find downstream dependents */}
								{(() => {
									const dependents = findDependents(selectedAsset, selectedColumn)
									if (dependents.length === 0) return null

									return (
										<>
											<div className="flex-shrink-0 text-gray-400">
												<svg
													className="w-8 h-8"
													fill="none"
													stroke="currentColor"
													viewBox="0 0 24 24"
												>
													<path
														strokeLinecap="round"
														strokeLinejoin="round"
														strokeWidth={2}
														d="M14 5l7 7m0 0l-7 7m7-7H3"
													/>
												</svg>
											</div>
											<div className="flex flex-col gap-2">
												{dependents.map((dep, i) => (
													<div
														key={i}
														className="px-3 py-2 bg-green-50 border border-green-200 rounded-lg text-sm"
													>
														<div className="text-green-600 font-medium text-xs">
															{dep.asset}
														</div>
														<code className="text-green-800">{dep.column}</code>
														{dep.example && (
															<div className="text-green-600 font-mono text-xs mt-1">
																{dep.example}
															</div>
														)}
													</div>
												))}
											</div>
										</>
									)
								})()}
							</div>
						)
					})()}
				</div>
			)}
		</div>
	)
}

export default LineagePage
