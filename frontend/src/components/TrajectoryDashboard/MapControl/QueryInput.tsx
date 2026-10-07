import { useState } from "react";
import { format } from "sql-formatter";
import { queryTrajectoryData } from "../../../utils/mobilityDb";
import { TrajectoryListAPI } from "../../../hooks/useTrajectoryList";
import type { UdfDef } from "../../../utils/queryBuilderTypes";
import QueryBuilder from "./QueryBuilder";

interface QueryInputProps {
	trajectoryList: TrajectoryListAPI;
	drawnPolygon: string | null;
	udfs: UdfDef[];
}

export default function QueryInput({
	trajectoryList,
	drawnPolygon,
	udfs,
}: QueryInputProps) {
	const [tab, setTab] = useState<"sql" | "builder">("sql");
	const [query, setQuery] = useState("");
	const [loading, setLoading] = useState(false);

	const runQuery = async (sql: string) => {
		setLoading(true);
		try {
			const data = await queryTrajectoryData(sql);
			trajectoryList.clear();
			trajectoryList.push(data);
		} finally {
			setLoading(false);
		}
	};

	const handleSqlRun = async () => {
		const formatted = format(query, { language: "postgresql" });
		setQuery(formatted);
		await runQuery(query);
	};

	const handleFormat = () => {
		if (!query.trim()) return;
		try {
			setQuery(format(query, { language: "postgresql" }));
		} catch {
			/* noop — let user fix syntax themselves */
		}
	};

	const handleCopyToEditor = (sql: string) => {
		setQuery(sql);
		setTab("sql");
	};

	return (
		<div className="border-b border-slate-200">
			{/* Tab bar */}
			<div className="tab-bar">
				<button
					onClick={() => setTab("sql")}
					data-active={tab === "sql"}
					className="tab"
				>
					<span className="material-symbols-outlined icon-sm mr-1.5">
						code
					</span>
					SQL Editor
				</button>
				<button
					onClick={() => setTab("builder")}
					data-active={tab === "builder"}
					className="tab"
				>
					<span className="material-symbols-outlined icon-sm mr-1.5">
						tune
					</span>
					Query Builder
				</button>
			</div>

			<div className={tab !== "sql" ? "hidden" : "p-3 flex flex-col gap-2"}>
				<div className="relative">
					<textarea
						value={query}
						onChange={(e) => setQuery(e.target.value)}
						className="textarea code min-h-[140px] bg-surface-code text-slate-100 placeholder:text-slate-500 border-slate-800 focus:border-accent focus:ring-accent/40"
						placeholder="SELECT * FROM csv.public.porto_taxi LIMIT 15"
						spellCheck={false}
					/>
					<button
						onClick={handleFormat}
						title="Format SQL"
						className="absolute top-2 right-2 icon-btn text-slate-400 hover:text-white hover:bg-slate-700"
					>
						<span className="material-symbols-outlined icon-sm">
							format_align_left
						</span>
					</button>
				</div>
				<div className="flex items-center gap-2">
					<button
						onClick={handleSqlRun}
						disabled={loading || !query.trim()}
						className="btn btn-primary flex-1"
					>
						{loading ? (
							<>
								<span className="material-symbols-outlined icon-sm animate-spin">
									progress_activity
								</span>
								Running…
							</>
						) : (
							<>
								<span className="material-symbols-outlined icon-sm">
									play_arrow
								</span>
								Run query
							</>
						)}
					</button>
					<button
						onClick={() => setQuery("")}
						disabled={!query}
						className="btn btn-secondary"
						title="Clear"
					>
						<span className="material-symbols-outlined icon-sm">
							backspace
						</span>
					</button>
				</div>
			</div>

			<div className={tab !== "builder" ? "hidden" : "p-3"}>
				<QueryBuilder
					drawnPolygon={drawnPolygon}
					loading={loading}
					udfs={udfs}
					onRun={runQuery}
					onCopyToEditor={handleCopyToEditor}
				/>
			</div>
		</div>
	);
}
