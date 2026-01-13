import { gql } from "urql"

// Get all assets with their latest materialization info
export const ASSETS_QUERY = gql`
	query AssetsQuery {
		assetsOrError {
			... on AssetConnection {
				nodes {
					id
					key {
						path
					}
					definition {
						description
						groupName
						computeKind
						opNames
					}
				}
			}
			... on PythonError {
				message
			}
		}
	}
`

// Get asset details including materializations
export const ASSET_DETAILS_QUERY = gql`
	query AssetDetailsQuery($assetKey: AssetKeyInput!) {
		assetOrError(assetKey: $assetKey) {
			... on Asset {
				id
				key {
					path
				}
				definition {
					description
					groupName
					computeKind
					metadata {
						key
						value
					}
				}
				assetMaterializations(limit: 10) {
					runId
					timestamp
					metadataEntries {
						label
						description
						... on IntMetadataEntry {
							intValue
						}
						... on FloatMetadataEntry {
							floatValue
						}
						... on TextMetadataEntry {
							text
						}
						... on MarkdownMetadataEntry {
							mdStr
						}
					}
				}
			}
			... on AssetNotFoundError {
				message
			}
		}
	}
`

// Get recent runs
export const RUNS_QUERY = gql`
	query RunsQuery($limit: Int!) {
		runsOrError(limit: $limit) {
			... on Runs {
				results {
					id
					runId
					status
					jobName
					startTime
					endTime
					tags {
						key
						value
					}
				}
			}
			... on PythonError {
				message
			}
		}
	}
`

// Get run details
export const RUN_DETAILS_QUERY = gql`
	query RunDetailsQuery($runId: ID!) {
		runOrError(runId: $runId) {
			... on Run {
				id
				runId
				status
				jobName
				startTime
				endTime
				stats {
					... on RunStatsSnapshot {
						stepsFailed
						stepsSucceeded
						materializations
						expectations
					}
				}
				tags {
					key
					value
				}
			}
			... on RunNotFoundError {
				message
			}
		}
	}
`

// Get jobs
export const JOBS_QUERY = gql`
	query JobsQuery {
		repositoriesOrError {
			... on RepositoryConnection {
				nodes {
					name
					location {
						name
					}
					jobs {
						name
						description
					}
				}
			}
		}
	}
`
