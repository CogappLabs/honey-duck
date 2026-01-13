import type { Metadata } from "next"
import { Geist, Geist_Mono } from "next/font/google"
import Link from "next/link"
import "./globals.css"
import { Providers } from "./providers"

const DAGSTER_URL = process.env.NEXT_PUBLIC_DAGSTER_URL || "http://127.0.0.1:3003"

const geistSans = Geist({
	variable: "--font-geist-sans",
	subsets: ["latin"],
})

const geistMono = Geist_Mono({
	variable: "--font-geist-mono",
	subsets: ["latin"],
})

export const metadata: Metadata = {
	title: "Honey Duck Dashboard",
	description: "Dagster pipeline dashboard for Honey Duck",
}

export default function RootLayout({
	children,
}: Readonly<{
	children: React.ReactNode
}>) {
	return (
		<html lang="en">
			<body className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
				<Providers>
					<div className="min-h-screen bg-gray-50">
						<nav className="bg-amber-600 text-white shadow-lg">
							<div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
								<div className="flex items-center justify-between h-16">
									<div className="flex items-center gap-8">
										<Link href="/" className="text-xl font-bold">
											Honey Duck
										</Link>
										<div className="flex gap-4">
											<Link href="/assets" className="hover:text-amber-200 transition-colors">
												Assets
											</Link>
											<Link href="/runs" className="hover:text-amber-200 transition-colors">
												Runs
											</Link>
											<Link href="/jobs" className="hover:text-amber-200 transition-colors">
												Jobs
											</Link>
										</div>
									</div>
									<a
										href={DAGSTER_URL}
										target="_blank"
										rel="noopener noreferrer"
										className="text-sm hover:text-amber-200 transition-colors"
									>
										Open Dagster UI
									</a>
								</div>
							</div>
						</nav>
						<main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">{children}</main>
					</div>
				</Providers>
			</body>
		</html>
	)
}
