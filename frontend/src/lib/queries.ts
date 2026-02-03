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
				}
				assetMaterializations(limit: 10) {
					runId
					timestamp
					metadataEntries {
						label
						... on IntMetadataEntry {
							intValue
						}
						... on FloatMetadataEntry {
							floatValue
						}
						... on TextMetadataEntry {
							text
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

// Get error events for a failed run
export const RUN_ERROR_QUERY = gql`
	query RunErrorQuery($runId: ID!) {
		runOrError(runId: $runId) {
			... on Run {
				runId
				eventConnection(afterCursor: null, limit: 50) {
					events {
						... on EngineEvent {
							message
							error {
								message
							}
						}
						... on RunFailureEvent {
							message
						}
					}
				}
			}
		}
	}
`

// Get run with materializations
export const RUN_WITH_ASSETS_QUERY = gql`
	query RunWithAssetsQuery($runId: ID!) {
		runOrError(runId: $runId) {
			... on Run {
				runId
				status
				jobName
				startTime
				endTime
				assetMaterializations {
					assetKey {
						path
					}
					timestamp
				}
			}
			... on RunNotFoundError {
				message
			}
		}
	}
`

// Get asset dependencies
export const ASSET_DEPENDENCIES_QUERY = gql`
	query AssetDependenciesQuery($assetKey: AssetKeyInput!) {
		assetOrError(assetKey: $assetKey) {
			... on Asset {
				key {
					path
				}
				definition {
					dependencyKeys {
						path
					}
					dependedByKeys {
						path
					}
				}
			}
		}
	}
`

// Get column lineage for an asset
export const COLUMN_LINEAGE_QUERY = gql`
	query ColumnLineageQuery($assetKey: AssetKeyInput!) {
		assetOrError(assetKey: $assetKey) {
			... on Asset {
				key {
					path
				}
				definition {
					description
					groupName
				}
				assetMaterializations(limit: 1) {
					timestamp
					metadataEntries {
						label
						... on TableColumnLineageMetadataEntry {
							lineage {
								columnName
								columnDeps {
									assetKey {
										path
									}
									columnName
								}
							}
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

// Get all assets with column lineage and example data
export const ALL_LINEAGE_QUERY = gql`
	query AllLineageQuery {
		assetsOrError {
			... on AssetConnection {
				nodes {
					key {
						path
					}
					definition {
						groupName
					}
					assetMaterializations(limit: 1) {
						metadataEntries {
							label
							... on TableColumnLineageMetadataEntry {
								lineage {
									columnName
									columnDeps {
										assetKey {
											path
										}
										columnName
									}
								}
							}
							... on JsonMetadataEntry {
								jsonString
							}
						}
					}
				}
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
