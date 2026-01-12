"use client"

import { Provider } from "urql"
import { createClient } from "@/lib/graphql"
import { useMemo } from "react"

export function Providers({ children }: { children: React.ReactNode }) {
	const client = useMemo(() => createClient(), [])

	return <Provider value={client}>{children}</Provider>
}
