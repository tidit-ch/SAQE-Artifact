import { useState, useEffect, useCallback } from "react";
import {
	fetchTables,
	fetchTableColumns,
	fetchCropFunctionDetails,
} from "../../../../utils/mobilityDb";
import { generateSql } from "../../../../utils/sqlGen";
import type {
	QueryBuilderState,
	TableInfo,
	ColumnInfo,
	UdfDef,
	CropFunctionDef,
} from "../../../../utils/queryBuilderTypes";
import ConditionGroupEditor from "./ConditionGroupEditor";
import CropProjectionEditor from "./CropProjectionEditor";

const EMPTY_STATE: QueryBuilderState = {
	table: null,
	selectedColumns: [],
	cropProjections: [],
	rootGroup: { id: "root", kind: "group", logic: "AND", children: [] },
	limit: "100",
};

interface QueryBuilderProps {
	drawnPolygon: string | null;
	loading: boolean;
	udfs: UdfDef[];
	onRun: (sql: string) => void;
	onCopyToEditor: (sql: string) => void;
}

export default function QueryBuilder({
	drawnPolygon,
	loading,
	udfs,
	onRun,
	onCopyToEditor,
}: QueryBuilderProps) {
	const [tables, setTables] = useState<TableInfo[]>([]);
	const [columns, setColumns] = useState<ColumnInfo[]>([]);
	const [state, setState] = useState<QueryBuilderState>(EMPTY_STATE);
	const [tablesLoading, setTablesLoading] = useState(false);
	const [columnsLoading, setColumnsLoading] = useState(false);
	const [columnFilter, setColumnFilter] = useState("");
	const [customColInput, setCustomColInput] = useState("");
	const [cropFunctions, setCropFunctions] = useState<CropFunctionDef[]>([]);

	const sql = generateSql(state, udfs, cropFunctions);

	useEffect(() => {
		setTablesLoading(true);
		fetchTables().then((t) => {
			setTables(t);
			setTablesLoading(false);
		});
		fetchCropFunctionDetails().then(setCropFunctions);
	}, []);

	useEffect(() => {
		if (!state.table) {
			setColumns([]);
			return;
		}
		setColumnsLoading(true);
		fetchTableColumns(state.table.fullName).then((cols) => {
			setColumns(cols);
			setColumnsLoading(false);
		});
	}, [state.table]);

	const handleTableChange = useCallback(
		(fullName: string) => {
			const t = tables.find((tb) => tb.fullName === fullName) ?? null;
			setState({ ...EMPTY_STATE, table: t, limit: "100" });
		},
		[tables],
	);

	const toggleColumn = (name: string) => {
		setState((prev) => ({
			...prev,
			selectedColumns: prev.selectedColumns.includes(name)
				? prev.selectedColumns.filter((c) => c !== name)
				: [...prev.selectedColumns, name],
		}));
	};

	const visibleColumns = columnFilter
		? columns.filter((c) =>
				c.name.toLowerCase().includes(columnFilter.toLowerCase()),
			)
		: columns;

	const columnNames = new Set(columns.map((c) => c.name));
	const customColumns = state.selectedColumns.filter(
		(c) => !columnNames.has(c),
	);

	const addCustomColumn = () => {
		const expr = customColInput.trim();
		if (!expr || state.selectedColumns.includes(expr)) return;
		setState((prev) => ({
			...prev,
			selectedColumns: [...prev.selectedColumns, expr],
		}));
		setCustomColInput("");
	};

	const allColumnsSelected =
		columns.length > 0 &&
		columns.every((c) => state.selectedColumns.includes(c.name));
	const selectedCount = state.selectedColumns.length;

	const isDirty =
		state.table !== null ||
		state.selectedColumns.length > 0 ||
		state.rootGroup.children.length > 0;

	return (
		<div className="flex flex-col gap-3">
			{/* Header row with clear button */}

			{/* FROM */}
			<div>
				<div className="flex items-center justify-between mb-1.5">
					<span className="field-label">From table</span>
					<button
						onClick={() => setState(EMPTY_STATE)}
						disabled={!isDirty}
						className="text-[10px] text-ink-500 hover:text-ink-900 disabled:opacity-30 disabled:cursor-not-allowed inline-flex items-center gap-0.5"
						title="Clear all"
					>
						<span className="material-symbols-outlined icon-sm">
							delete_sweep
						</span>
						Clear all
					</button>
				</div>
				{tablesLoading ? (
					<div className="hint flex items-center gap-1.5">
						<span className="material-symbols-outlined icon-sm animate-spin">
							progress_activity
						</span>
						Loading tables…
					</div>
				) : (
					<select
						value={state.table?.fullName ?? ""}
						onChange={(e) => handleTableChange(e.target.value)}
						className="select"
					>
						<option value="">— select a table —</option>
						{tables.map((t) => (
							<option key={t.fullName} value={t.fullName}>
								{t.fullName}
							</option>
						))}
					</select>
				)}
			</div>

			{/* SELECT */}
			{state.table && (
				<div>
					<div className="flex items-center justify-between mb-1.5">
						<span className="field-label">
							Columns
							{selectedCount > 0 && (
								<span className="ml-1.5 normal-case font-normal text-ink-400">
									{selectedCount} selected
								</span>
							)}
						</span>
						<button
							onClick={() =>
								setState((prev) => ({
									...prev,
									selectedColumns: allColumnsSelected
										? prev.selectedColumns.filter((c) => !columnNames.has(c))
										: [
												...new Set([
													...prev.selectedColumns,
													...columns.map((c) => c.name),
												]),
											],
								}))
							}
							className="text-[10px] text-accent hover:text-accent-hover"
						>
							{allColumnsSelected ? "deselect all" : "select all (*)"}
						</button>
					</div>
					{columnsLoading ? (
						<div className="hint flex items-center gap-1.5">
							<span className="material-symbols-outlined icon-sm animate-spin">
								progress_activity
							</span>
							Loading columns…
						</div>
					) : (
						<>
							{columns.length > 6 && (
								<div className="relative mb-1.5">
									<span className="material-symbols-outlined icon-sm absolute left-2 top-1/2 -translate-y-1/2 text-ink-400 pointer-events-none">
										search
									</span>
									<input
										value={columnFilter}
										onChange={(e) => setColumnFilter(e.target.value)}
										placeholder="Filter columns…"
										className="input pl-7 h-7 text-[12px]"
									/>
								</div>
							)}
							<div className="rounded-md border border-slate-200 bg-slate-50/40 max-h-44 overflow-y-auto divide-y divide-slate-100">
								{visibleColumns.map((col) => {
									const checked = state.selectedColumns.includes(col.name);
									return (
										<label
											key={col.name}
											className="flex items-center gap-2 px-2 py-1 text-[12px] cursor-pointer hover:bg-white"
										>
											<input
												type="checkbox"
												checked={checked}
												onChange={() => toggleColumn(col.name)}
												className="checkbox"
											/>
											<span className="truncate flex-1 min-w-0 text-ink-900 font-medium">
												{col.name}
											</span>
											<span
												className="text-2xs text-ink-400 truncate font-mono shrink-0 max-w-[40%] cursor-copy"
												title={col.type}
												onClick={(e) => {
													e.preventDefault();
													navigator.clipboard.writeText(col.type);
												}}
											>
												{col.type}
											</span>
										</label>
									);
								})}
								{visibleColumns.length === 0 && (
									<div className="hint p-2 text-center">No columns match.</div>
								)}
							</div>
						</>
					)}
					{/* Custom expressions */}
					<div className="mt-2">
						{customColumns.length > 0 && (
							<div className="flex flex-wrap gap-1 mb-1.5">
								{customColumns.map((expr) => (
									<span
										key={expr}
										className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-accent/10 text-accent text-[11px] font-mono"
									>
										{expr}
										<button
											onClick={() => toggleColumn(expr)}
											className="hover:text-red-500 leading-none"
											title="Remove"
										>
											<span
												className="material-symbols-outlined icon-sm"
												style={{ fontSize: 12 }}
											>
												close
											</span>
										</button>
									</span>
								))}
							</div>
						)}
						<div className="flex gap-1.5">
							<input
								value={customColInput}
								onChange={(e) => setCustomColInput(e.target.value)}
								onKeyDown={(e) => e.key === "Enter" && addCustomColumn()}
								placeholder="e.g. count(*), ST_Length(geom)"
								className="input flex-1 h-7 text-[12px] font-mono"
							/>
							<button
								onClick={addCustomColumn}
								disabled={!customColInput.trim()}
								className="btn btn-secondary h-7 px-2 text-[11px]"
							>
								Add
							</button>
						</div>
					</div>
				</div>
			)}

			{/* CROP projections */}
			{state.table && (
				<div>
					<div className="field-label mb-1.5">Crop projections</div>
					<CropProjectionEditor
						projections={state.cropProjections}
						cropFunctions={cropFunctions}
						columns={columns}
						drawnPolygon={drawnPolygon}
						onChange={(cropProjections) =>
							setState((prev) => ({ ...prev, cropProjections }))
						}
					/>
				</div>
			)}

			{/* WHERE */}
			{state.table && (
				<div>
					<div className="field-label mb-1.5">Filter conditions</div>
					<ConditionGroupEditor
						group={state.rootGroup}
						columns={columns}
						udfs={udfs}
						drawnPolygon={drawnPolygon}
						isRoot={true}
						onChange={(updated) =>
							setState((prev) => ({ ...prev, rootGroup: updated }))
						}
					/>
				</div>
			)}

			{/* LIMIT */}
			{state.table && (
				<div>
					<div className="field-label mb-1.5">Row limit</div>
					<input
						type="number"
						min="1"
						value={state.limit}
						onChange={(e) =>
							setState((prev) => ({ ...prev, limit: e.target.value }))
						}
						className="input w-32"
					/>
				</div>
			)}

			{/* SQL preview */}
			{sql && (
				<div>
					<div className="flex items-center justify-between mb-1.5">
						<span className="field-label">Preview SQL</span>
						<button
							onClick={() => navigator.clipboard.writeText(sql)}
							className="text-[10px] text-ink-500 hover:text-ink-900 inline-flex items-center gap-0.5"
							title="Copy to clipboard"
						>
							<span className="material-symbols-outlined icon-sm">
								content_copy
							</span>
							copy
						</button>
					</div>
					<pre className="code-block">{sql}</pre>
				</div>
			)}

			{/* Actions */}
			{sql && (
				<div className="flex gap-2 sticky bottom-0 -mx-3 -mb-3 px-3 py-2 bg-gradient-to-t from-white via-white to-white/0 border-t border-slate-100">
					<button
						onClick={() => onRun(sql)}
						disabled={loading}
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
						onClick={() => onCopyToEditor(sql)}
						className="btn btn-secondary"
						title="Move to SQL editor"
					>
						<span className="material-symbols-outlined icon-sm">edit_note</span>
						Edit as SQL
					</button>
				</div>
			)}
		</div>
	);
}
