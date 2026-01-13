const STATUS_COLORS: Record<string, string> = {
	SUCCESS: "bg-green-100 text-green-800",
	FAILURE: "bg-red-100 text-red-800",
	STARTED: "bg-blue-100 text-blue-800",
	QUEUED: "bg-yellow-100 text-yellow-800",
	CANCELED: "bg-gray-100 text-gray-800",
	CANCELING: "bg-orange-100 text-orange-800",
}

export const StatusBadge = ({ status }: { status: string }) => (
	<span
		className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${STATUS_COLORS[status] || "bg-gray-100 text-gray-800"}`}
	>
		{status}
	</span>
)
