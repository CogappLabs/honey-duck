"use client"

import { useQuery } from "urql"
import { useSearchParams } from "next/navigation"
import Link from "next/link"
import { ASSETS_QUERY } from "@/lib/queries"
import { Suspense } from "react"
import type { Asset } from "@/lib/types"

const AssetsContent = () => {
	const searchParams = useSearchParams()
	const groupFilter = searchParams.get("group")

	const [result] = useQuery({ query: ASSETS_QUERY })

	const assets: Asset[] =
		result.data?.assetsOrError?.__typename === "AssetConnection"
			? result.data.assetsOrError.nodes
			: []

	// Group assets
	const assetsByGroup = assets.reduce(
		(acc: Record<string, Asset[]>, asset: Asset) => {
			const group = asset.definition?.groupName || "ungrouped"
			if (!acc[group]) acc[group] = []
			acc[group].push(asset)
			return acc
		},
		{} as Record<string, Asset[]>
	)

	// Filter by group if specified
	const filteredGroups: Record<string, Asset[]> = groupFilter
		? { [groupFilter]: assetsByGroup[groupFilter] || [] }
		: assetsByGroup

	const allGroups = Object.keys(assetsByGroup).sort()

	return (
		<div className="space-y-6">
			<div className="flex justify-between items-center">
				<div>
					<h1 className="text-3xl font-bold text-gray-900">Assets</h1>
					<p className="mt-1 text-gray-600">
						{assets.length} assets in {allGroups.length} groups
					</p>
				</div>
			</div>

			{/* Group filter tabs */}
			<div className="flex flex-wrap gap-2">
				<Link
					href="/assets"
					className={`px-3 py-1 rounded-full text-sm ${
						!groupFilter
							? "bg-amber-600 text-white"
							: "bg-gray-200 text-gray-700 hover:bg-gray-300"
					}`}
				>
					All
				</Link>
				{allGroups.map((group) => (
					<Link
						key={group}
						href={`/assets?group=${group}`}
						className={`px-3 py-1 rounded-full text-sm ${
							groupFilter === group
								? "bg-amber-600 text-white"
								: "bg-gray-200 text-gray-700 hover:bg-gray-300"
						}`}
					>
						{group} ({assetsByGroup[group].length})
					</Link>
				))}
			</div>

			{result.fetching ? (
				<div className="text-gray-500">Loading assets...</div>
			) : result.error ? (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<p className="text-red-600">
						Error loading assets. Is Dagster running?
					</p>
					<p className="text-sm text-red-500 mt-1">{result.error.message}</p>
				</div>
			) : (
				<div className="space-y-8">
					{Object.entries(filteredGroups).map(([group, groupAssets]) => (
						<div key={group}>
							<h2 className="text-xl font-semibold text-gray-900 mb-4">{group}</h2>
							<div className="bg-white rounded-lg shadow overflow-hidden">
								<table className="min-w-full divide-y divide-gray-200">
									<thead className="bg-gray-50">
										<tr>
											<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
												Asset
											</th>
											<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
												Kind
											</th>
											<th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
												Description
											</th>
										</tr>
									</thead>
									<tbody className="bg-white divide-y divide-gray-200">
										{groupAssets.map((asset) => (
											<tr key={asset.id} className="hover:bg-gray-50">
												<td className="px-6 py-4 whitespace-nowrap">
													<Link
														href={`/assets/${asset.key.path.join("/")}`}
														className="text-amber-600 hover:text-amber-700 font-medium"
													>
														{asset.key.path.join(" / ")}
													</Link>
												</td>
												<td className="px-6 py-4 whitespace-nowrap">
													{asset.definition?.computeKind && (
														<span className="px-2 py-1 text-xs bg-gray-100 rounded">
															{asset.definition.computeKind}
														</span>
													)}
												</td>
												<td className="px-6 py-4 text-sm text-gray-500 max-w-md truncate">
													{asset.definition?.description || "-"}
												</td>
											</tr>
										))}
									</tbody>
								</table>
							</div>
						</div>
					))}
				</div>
			)}
		</div>
	)
}

const AssetsPage = () => (
	<Suspense fallback={<div className="text-gray-500">Loading...</div>}>
		<AssetsContent />
	</Suspense>
)

export default AssetsPage
