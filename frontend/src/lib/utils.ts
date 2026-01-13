export function formatDuration(seconds: number): string {
	if (seconds < 60) {
		return `${Math.round(seconds)}s`
	}
	const minutes = Math.floor(seconds / 60)
	const secs = Math.round(seconds % 60)
	if (minutes < 60) {
		return `${minutes}m ${secs}s`
	}
	const hours = Math.floor(minutes / 60)
	const mins = minutes % 60
	return `${hours}h ${mins}m`
}

export function formatDateTime(timestamp: number): string {
	const date = new Date(timestamp * 1000)
	const now = new Date()
	const isToday = date.toDateString() === now.toDateString()

	const time = date.toLocaleTimeString("en-GB", {
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit"
	})

	if (isToday) {
		return `Today ${time}`
	}

	const dateStr = date.toLocaleDateString("en-GB", {
		day: "numeric",
		month: "short",
	})

	return `${dateStr} ${time}`
}

export function formatRelativeTime(timestamp: number): string {
	const seconds = Math.floor(Date.now() / 1000 - timestamp)

	if (seconds < 60) return "just now"
	if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
	if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
	if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`

	return formatDateTime(timestamp)
}
