"use client"

import { useQuery } from "urql"
import { JOBS_QUERY } from "@/lib/queries"
import { DAGSTER_URL } from "@/lib/config"
import type { Repository } from "@/lib/types"

const JobsPage = () => {
	const [result] = useQuery({ query: JOBS_QUERY })

	const allRepositories: Repository[] =
		result.data?.repositoriesOrError?.__typename === "RepositoryConnection"
			? result.data.repositoriesOrError.nodes
			: []

	// Filter out internal jobs and repositories with no visible jobs
	const repositories = allRepositories
		.map((repo) => ({
			...repo,
			jobs: repo.jobs.filter((job) => !job.name.startsWith("__")),
		}))
		.filter((repo) => repo.jobs.length > 0)

	return (
		<div className="space-y-6">
			<div>
				<h1 className="text-3xl font-bold text-gray-900">Jobs</h1>
				<p className="mt-1 text-gray-600">Available pipeline jobs</p>
			</div>

			{result.fetching ? (
				<div className="text-gray-500">Loading jobs...</div>
			) : result.error ? (
				<div className="bg-red-50 border border-red-200 rounded-lg p-4">
					<p className="text-red-600">Error loading jobs. Is Dagster running?</p>
					<p className="text-sm text-red-500 mt-1">{result.error.message}</p>
				</div>
			) : repositories.length === 0 ? (
				<div className="bg-white rounded-lg shadow p-8 text-center text-gray-500">
					No jobs found. Make sure you have jobs defined in your Dagster definitions.
				</div>
			) : (
				<div className="space-y-8">
					{repositories.map((repo) => (
						<div key={repo.name}>
							{!repo.name.startsWith("__") && (
								<h2 className="text-xl font-semibold text-gray-900 mb-4">{repo.name}</h2>
							)}
							<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
								{repo.jobs.map((job) => (
									<div key={job.name} className="bg-white rounded-lg shadow p-4">
										<div className="flex justify-between items-start">
											<div>
												<h3 className="font-medium text-gray-900">{job.name}</h3>
												{job.description && (
													<p className="mt-1 text-sm text-gray-500">{job.description}</p>
												)}
											</div>
											<a
												href={`${DAGSTER_URL}/locations/${repo.location.name}/jobs/${job.name}`}
												target="_blank"
												rel="noopener noreferrer"
												className="text-sm text-amber-600 hover:text-amber-700"
											>
												Launch
											</a>
										</div>
									</div>
								))}
							</div>
						</div>
					))}
				</div>
			)}
		</div>
	)
}

export default JobsPage
