const SAMPLE_QUERIES: { label: string; sql: string }[] = [
	{
		label: "Latest 15 trips",
		sql: "SELECT * FROM csv.public.porto_taxi LIMIT 15",
	},
	{
		label: "Strictly contained in polygon",
		sql: "SELECT * FROM csv.public.porto_taxi WHERE contained(polyline, 'strict', 'POLYGON((41.15097 -8.648829, 41.177367 -8.648829, 41.177367 -8.593668, 41.15097 -8.593668))') LIMIT 50",
	},
	{
		label: "Properly contained in polygon",
		sql: "SELECT * FROM csv.public.porto_taxi WHERE properly_contained(polyline, 'POLYGON((41.15097 -8.648829, 41.177367 -8.648829, 41.177367 -8.593668, 41.15097 -8.593668))') LIMIT 50",
	},
];

export default function SampleQueries() {
	const handleCopy = (sql: string) => {
		navigator.clipboard.writeText(sql);
	};

	return (
		<div className="flex flex-col gap-1.5">
			{SAMPLE_QUERIES.map((q, i) => (
				<div
					key={i}
					className="rounded-md border border-slate-200 bg-white overflow-hidden"
				>
					<div className="flex items-center justify-between px-2 py-1 bg-slate-50/60 border-b border-slate-200">
						<span className="text-[11.5px] font-semibold text-ink-700 truncate">
							{q.label}
						</span>
						<button
							onClick={() => handleCopy(q.sql)}
							className="icon-btn-inline icon-btn--accent"
							title="Copy SQL"
						>
							<span className="material-symbols-outlined icon-sm">
								content_copy
							</span>
						</button>
					</div>
					<pre className="code text-ink-700 px-2 py-1.5 whitespace-pre-wrap break-all text-[11px] leading-snug">
						{q.sql}
					</pre>
				</div>
			))}
		</div>
	);
}
