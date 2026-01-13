export type Asset = {
	id: string
	key: { path: string[] }
	definition?: {
		groupName?: string
		description?: string
		computeKind?: string
	}
}

export type Run = {
	runId: string
	jobName: string
	status: string
	startTime: number | null
	endTime?: number | null
	error?: {
		message: string
	} | null
}

export type Job = {
	name: string
	description?: string
}

export type Repository = {
	name: string
	location: { name: string }
	jobs: Job[]
}
