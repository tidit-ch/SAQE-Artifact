import { useState } from "react";
import type { UdfDef } from "../../../../utils/queryBuilderTypes";

interface UdfBrowserProps {
	registeredUdfs: UdfDef[];
}

const CATEGORY_LABELS: Record<string, string> = {
	spatial: "Spatial",
	temporal: "Temporal",
	spatio_temporal: "Spatio-Temporal",
	geodata: "Geodata",
	utility: "Utility",
	map_matching: "Map Matching",
};

const CATEGORY_ORDER: UdfDef["category"][] = [
	"spatial",
	"temporal",
	"spatio_temporal",
	"geodata",
	"utility",
	"map_matching",
];

export default function UdfBrowser({ registeredUdfs }: UdfBrowserProps) {
	const [expandedUdf, setExpandedUdf] = useState<string | null>(null);
	const [search, setSearch] = useState("");

	const registeredSet = new Set(registeredUdfs.map((u) => u.name));

	const filtered = registeredUdfs.filter(
		(u) =>
			u.name.includes(search.toLowerCase()) ||
			u.description.toLowerCase().includes(search.toLowerCase()),
	);

	const grouped = filtered.reduce(
		(acc, udf) => {
			(acc[udf.category] ??= []).push(udf);
			return acc;
		},
		{} as Record<string, UdfDef[]>,
	);

	return (
		<div className="rounded-md border border-slate-200 bg-slate-50/40 overflow-hidden">
			<div className="p-2 flex flex-col gap-2 border-t border-slate-200 bg-white">
				<div className="relative">
					<span className="material-symbols-outlined icon-sm absolute left-2 top-1/2 -translate-y-1/2 text-ink-400 pointer-events-none">
						search
					</span>
					<input
						type="text"
						value={search}
						onChange={(e) => setSearch(e.target.value)}
						placeholder="Search functions…"
						className="input pl-7 h-7 text-[12px]"
					/>
				</div>

				{CATEGORY_ORDER.map((cat) => {
					const udfs = grouped[cat];
					if (!udfs?.length) return null;
					return (
						<div key={cat}>
							<p className="field-label mb-1">{CATEGORY_LABELS[cat]}</p>
							<div className="flex flex-col gap-0.5">
								{udfs.map((udf) => {
									const isExpanded = expandedUdf === udf.name;
									const isRegistered = registeredSet.has(udf.name);
									return (
										<div
											key={udf.name}
											className="rounded border border-slate-200 bg-white overflow-hidden"
										>
											<button
												onClick={() =>
													setExpandedUdf(isExpanded ? null : udf.name)
												}
												className="flex flex-row items-center justify-between w-full px-2 py-1.5 text-left hover:bg-slate-50"
											>
												<div className="flex items-center gap-1.5 min-w-0">
													<span className="material-symbols-outlined icon-sm text-ink-400">
														{isExpanded ? "expand_more" : "chevron_right"}
													</span>
													<code className="text-[11.5px] font-mono text-accent truncate">
														{udf.name}
													</code>
													{!isRegistered && (
														<span className="badge badge--amber">
															not available
														</span>
													)}
												</div>
												<span className="text-2xs text-ink-400 shrink-0 ml-2 font-mono">
													→ {udf.returnType}
												</span>
											</button>

											{isExpanded && (
												<div className="px-2.5 pb-2 pt-1 flex flex-col gap-2 border-t border-slate-100 bg-slate-50/40">
													<p className="text-[11.5px] text-ink-700 leading-snug">
														{udf.description}
													</p>
													<div>
														<p className="field-label mb-0.5">Syntax</p>
														<code className="code-block--light block">
															{udf.syntax}
														</code>
													</div>
													<div>
														<p className="field-label mb-0.5">Parameters</p>
														<ul className="flex flex-col gap-0.5 text-[11.5px]">
															{udf.params.map((p) => (
																<li
																	key={p.name}
																	className="text-ink-700 leading-snug"
																>
																	<code className="font-mono text-ink-900">
																		{p.name}
																	</code>
																	<span className="text-ink-400 font-mono">
																		{" "}
																		{p.type}
																	</span>
																	<span className="text-ink-500">
																		{" "}
																		— {p.description}
																	</span>
																</li>
															))}
														</ul>
													</div>
													{udf.examples && udf.examples.length > 0 && (
														<div>
															<p className="field-label mb-0.5">Example</p>
															{udf.examples.map((ex, i) => (
																<code
																	key={i}
																	className="code-block--light block mt-0.5"
																>
																	{ex}
																</code>
															))}
														</div>
													)}
												</div>
											)}
										</div>
									);
								})}
							</div>
						</div>
					);
				})}
			</div>
		</div>
	);
}
