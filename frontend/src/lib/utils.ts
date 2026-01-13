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
