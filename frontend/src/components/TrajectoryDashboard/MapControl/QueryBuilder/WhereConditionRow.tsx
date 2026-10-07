import type {
	WhereCondition,
	ColumnInfo,
	ConditionOperator,
	UdfDef,
} from "../../../../utils/queryBuilderTypes";

const OPERATORS: ConditionOperator[] = [
	"=",
	"!=",
	">",
	">=",
	"<",
	"<=",
	"LIKE",
	"IS NULL",
	"IS NOT NULL",
];

const CATEGORY_LABELS: Record<string, string> = {
	spatial: "Spatial",
	temporal: "Temporal",
	spatio_temporal: "Spatio-Temporal",
	geodata: "Geodata",
	utility: "Utility",
};

const CATEGORY_ORDER = [
	"spatial",
	"temporal",
	"spatio_temporal",
	"geodata",
	"utility",
];

interface WhereConditionRowProps {
	condition: WhereCondition;
	columns: ColumnInfo[];
	udfs: UdfDef[];
	drawnPolygon: string | null;
	onChange: (updated: WhereCondition) => void;
	onRemove: () => void;
}

function isTrajectoryColumn(col: ColumnInfo): boolean {
	return col.type.toLowerCase().startsWith("list");
}

function defaultForParamType(type: string): string {
	if (type === "strictness") return "strict";
	if (type === "time_unit") return "s";
	return "";
}

export default function WhereConditionRow({
	condition,
	columns,
	udfs,
	drawnPolygon,
	onChange,
	onRemove,
}: WhereConditionRowProps) {
	const trajectoryColumns = columns.filter(isTrajectoryColumn);

	// ── Simple condition ──────────────────────────────────────────────────────
	if (condition.kind === "simple") {
		const needsValue =
			condition.operator !== "IS NULL" && condition.operator !== "IS NOT NULL";
		return (
			<div className="well group/row">
				<div className="flex flex-row items-center gap-1.5">
					<select
						value={condition.column}
						onChange={(e) => onChange({ ...condition, column: e.target.value })}
						className="select flex-[2] min-w-0"
					>
						<option value="">— column —</option>
						{columns.map((c) => (
							<option key={c.name} value={c.name}>
								{c.name} ({c.type})
							</option>
						))}
					</select>
					<select
						value={condition.operator}
						onChange={(e) =>
							onChange({
								...condition,
								operator: e.target.value as ConditionOperator,
							})
						}
						className="select w-auto pr-7 font-mono text-[12px]"
					>
						{OPERATORS.map((op) => (
							<option key={op} value={op}>
								{op}
							</option>
						))}
					</select>
					{needsValue && (
						<input
							type="text"
							value={condition.value}
							onChange={(e) => onChange({ ...condition, value: e.target.value })}
							placeholder="value"
							className="input flex-[2] min-w-0"
						/>
					)}
					<button
						onClick={onRemove}
						className="icon-btn-inline"
						title="Remove condition"
					>
						<span className="material-symbols-outlined icon-sm">close</span>
					</button>
				</div>
			</div>
		);
	}

	// ── UDF condition ─────────────────────────────────────────────────────────
	const selectedUdf = udfs.find((u) => u.name === condition.udfName);

	const updateArg = (i: number, val: string) => {
		const newArgs = [...condition.args];
		newArgs[i] = val;
		onChange({ ...condition, args: newArgs });
	};

	const handleUdfChange = (name: string) => {
		const udf = udfs.find((u) => u.name === name);
		onChange({
			...condition,
			udfName: name,
			args: udf ? udf.params.map((p) => defaultForParamType(p.type)) : [],
		});
	};

	const grouped = CATEGORY_ORDER.reduce(
		(acc, cat) => {
			const matches = udfs.filter((u) => u.category === cat);
			if (matches.length > 0) acc[cat] = matches;
			return acc;
		},
		{} as Record<string, UdfDef[]>,
	);

	return (
		<div className="well">
			<div className="flex items-center gap-1.5 mb-1.5">
				<span className="material-symbols-outlined icon-sm text-accent">
					function
				</span>
				<select
					value={condition.udfName}
					onChange={(e) => handleUdfChange(e.target.value)}
					className="select flex-1 min-w-0 font-mono text-[12px]"
				>
					<option value="">— select function —</option>
					{Object.entries(grouped).map(([cat, catUdfs]) => (
						<optgroup key={cat} label={CATEGORY_LABELS[cat] ?? cat}>
							{catUdfs.map((u) => (
								<option key={u.name} value={u.name}>
									{u.name}
								</option>
							))}
						</optgroup>
					))}
				</select>
				<button
					onClick={onRemove}
					className="icon-btn-inline"
					title="Remove condition"
				>
					<span className="material-symbols-outlined icon-sm">close</span>
				</button>
			</div>

			{selectedUdf && (
				<>
					<p className="hint mb-2 italic leading-snug">
						{selectedUdf.description}
					</p>
					<div className="flex flex-col gap-1.5">
						{selectedUdf.params.map((param, i) => {
							const value = condition.args[i] ?? "";
							return (
								<div key={param.name} className="flex flex-col gap-0.5">
									<label className="text-2xs font-medium text-ink-700">
										{param.name}
										<span className="ml-1 font-mono font-normal text-ink-400">
											{param.type}
										</span>
									</label>

									{param.type === "trajectory_column" && (
										<select
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											className="select"
										>
											<option value="">— column —</option>
											{(trajectoryColumns.length > 0
												? trajectoryColumns
												: columns
											).map((c) => (
												<option key={c.name} value={c.name}>
													{c.name}
												</option>
											))}
										</select>
									)}

									{param.type === "column" && (
										<select
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											className="select"
										>
											<option value="">— column —</option>
											{columns.map((c) => (
												<option key={c.name} value={c.name}>
													{c.name} ({c.type})
												</option>
											))}
										</select>
									)}

									{param.type === "polygon_wkt" && (
										<div className="flex flex-col gap-1">
											<textarea
												value={value}
												onChange={(e) => updateArg(i, e.target.value)}
												placeholder="POLYGON((lat1 lon1, lat2 lon2, ...))"
												rows={3}
												className="textarea code"
											/>
											{drawnPolygon && (
												<button
													onClick={() => updateArg(i, drawnPolygon)}
													className="btn btn-secondary btn-xs self-start"
												>
													<span className="material-symbols-outlined icon-sm">
														hexagon
													</span>
													Use drawn polygon
												</button>
											)}
										</div>
									)}

									{param.type === "strictness" && (
										<select
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											className="select"
										>
											<option value="strict">strict</option>
											<option value="relaxed">relaxed</option>
										</select>
									)}

									{param.type === "time_unit" && (
										<select
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											className="select"
										>
											<option value="ms">ms</option>
											<option value="s">s</option>
											<option value="min">min</option>
											<option value="h">h</option>
										</select>
									)}

									{param.type === "timestamp_ms" && (
										<input
											type="datetime-local"
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											className="input"
										/>
									)}

									{param.type === "integer" && (
										<input
											type="number"
											step="1"
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											placeholder="0"
											className="input"
										/>
									)}

									{param.type === "float" && (
										<input
											type="number"
											step="any"
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											placeholder="0"
											className="input"
										/>
									)}

									{param.type === "string" && (
										<input
											type="text"
											value={value}
											onChange={(e) => updateArg(i, e.target.value)}
											placeholder={param.description}
											className="input"
										/>
									)}
								</div>
							);
						})}
					</div>
				</>
			)}
		</div>
	);
}
