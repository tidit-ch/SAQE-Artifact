import type {
	CropExprGroup,
	CropExprNode,
	CropFunctionCall,
	CropFunctionDef,
} from "../../../../utils/queryBuilderTypes";

const newId = () =>
	crypto.randomUUID?.() ??
	`${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

function newCall(): CropFunctionCall {
	return { id: newId(), kind: "call", functionName: "", args: [] };
}

function newGroup(): CropExprGroup {
	return { id: newId(), kind: "group", logic: "OR", children: [] };
}

// ── Single call row ──────────────────────────────────────────────────────────

interface CropCallRowProps {
	call: CropFunctionCall;
	cropFunctions: CropFunctionDef[];
	drawnPolygon: string | null;
	onChange: (updated: CropFunctionCall) => void;
	onRemove: () => void;
}

function CropCallRow({
	call,
	cropFunctions,
	drawnPolygon,
	onChange,
	onRemove,
}: CropCallRowProps) {
	const funcDef = cropFunctions.find((f) => f.name === call.functionName);

	const handleFunctionChange = (name: string) => {
		const def = cropFunctions.find((f) => f.name === name);
		onChange({
			...call,
			functionName: name,
			args: def ? def.args.map(() => "") : [],
		});
	};

	const updateArg = (i: number, val: string) => {
		const newArgs = [...call.args];
		newArgs[i] = val;
		onChange({ ...call, args: newArgs });
	};

	return (
		<div className="well">
			<div className="flex items-center gap-1.5 mb-1.5">
				<span className="material-symbols-outlined icon-sm text-accent">
					function
				</span>
				<select
					value={call.functionName}
					onChange={(e) => handleFunctionChange(e.target.value)}
					className="select flex-1 min-w-0 font-mono text-[12px]"
				>
					<option value="">— select function —</option>
					{cropFunctions.map((f) => (
						<option key={f.name} value={f.name}>
							{f.name}
						</option>
					))}
				</select>
				<button onClick={onRemove} className="icon-btn-inline" title="Remove">
					<span className="material-symbols-outlined icon-sm">close</span>
				</button>
			</div>

			{funcDef && (
				<>
					{funcDef.description && (
						<p className="hint mb-2 italic leading-snug">{funcDef.description}</p>
					)}
					<div className="flex flex-col gap-1.5">
						{funcDef.args.map((arg, i) => {
							const val = call.args[i] ?? "";
							return (
								<div key={arg.name} className="flex flex-col gap-0.5">
									<label className="text-2xs font-medium text-ink-700">
										{arg.name}
										<span className="ml-1 font-mono font-normal text-ink-400">
											{arg.type}
										</span>
									</label>

									{arg.type === "polygon_wkt" && (
										<div className="flex flex-col gap-1">
											<textarea
												value={val}
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

									{arg.type === "float" && (
										<input
											type="number"
											step="any"
											value={val}
											onChange={(e) => updateArg(i, e.target.value)}
											placeholder="0"
											className="input"
										/>
									)}

									{arg.type === "integer" && (
										<input
											type="number"
											step="1"
											value={val}
											onChange={(e) => updateArg(i, e.target.value)}
											placeholder="0"
											className="input"
										/>
									)}

									{arg.type === "timestamp_ms" && (
										<input
											type="datetime-local"
											value={val}
											onChange={(e) => updateArg(i, e.target.value)}
											className="input"
										/>
									)}

									{arg.type === "string" && (
										<input
											type="text"
											value={val}
											onChange={(e) => updateArg(i, e.target.value)}
											placeholder={arg.description}
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

// ── Recursive group editor ───────────────────────────────────────────────────

interface CropExprEditorProps {
	group: CropExprGroup;
	cropFunctions: CropFunctionDef[];
	drawnPolygon: string | null;
	isRoot?: boolean;
	onChange: (updated: CropExprGroup) => void;
	onRemove?: () => void;
}

export default function CropExprEditor({
	group,
	cropFunctions,
	drawnPolygon,
	isRoot = false,
	onChange,
	onRemove,
}: CropExprEditorProps) {
	const updateChild = (id: string, updated: CropExprNode) =>
		onChange({
			...group,
			children: group.children.map((c) => (c.id === id ? updated : c)),
		});

	const removeChild = (id: string) =>
		onChange({ ...group, children: group.children.filter((c) => c.id !== id) });

	const addCall = () =>
		onChange({ ...group, children: [...group.children, newCall()] });

	const addSubGroup = () =>
		onChange({ ...group, children: [...group.children, newGroup()] });

	const logicToggle = (
		<div className="seg">
			{(["AND", "OR"] as const).map((op) => (
				<button
					key={op}
					onClick={() => onChange({ ...group, logic: op })}
					data-active={group.logic === op}
					className="seg-btn"
				>
					{op}
				</button>
			))}
		</div>
	);

	const childrenEl = (
		<div className="flex flex-col gap-1.5">
			{group.children.map((child, index) => (
				<div key={child.id}>
					{index > 0 && (
						<div className="flex items-center gap-2 my-1">
							<div className="flex-1 border-t border-dashed border-slate-300" />
							<span className="text-2xs font-bold text-accent bg-accent-soft px-1.5 py-0.5 rounded shrink-0 tracking-wide">
								{group.logic}
							</span>
							<div className="flex-1 border-t border-dashed border-slate-300" />
						</div>
					)}

					{child.kind === "group" ? (
						<CropExprEditor
							group={child}
							cropFunctions={cropFunctions}
							drawnPolygon={drawnPolygon}
							isRoot={false}
							onChange={(updated) => updateChild(child.id, updated)}
							onRemove={() => removeChild(child.id)}
						/>
					) : (
						<CropCallRow
							call={child}
							cropFunctions={cropFunctions}
							drawnPolygon={drawnPolygon}
							onChange={(updated) => updateChild(child.id, updated)}
							onRemove={() => removeChild(child.id)}
						/>
					)}
				</div>
			))}

			{group.children.length === 0 && (
				<div className="rounded-md border border-dashed border-slate-300 bg-slate-50/40 px-3 py-3 text-center hint">
					Add a crop function call below ↓
				</div>
			)}
		</div>
	);

	const addButtons = (
		<div className="flex items-center gap-1 mt-1.5">
			<button onClick={addCall} className="btn btn-secondary btn-sm flex-1">
				<span className="material-symbols-outlined icon-sm">add</span>
				Function
			</button>
			<button
				onClick={addSubGroup}
				className="btn btn-secondary btn-sm"
				title="Add nested group"
			>
				<span className="material-symbols-outlined icon-sm">account_tree</span>
				Group
			</button>
		</div>
	);

	if (isRoot) {
		return (
			<div className="flex flex-col gap-1.5">
				{group.children.length > 1 && (
					<div className="flex items-center gap-2 mb-0.5">
						<span className="hint">Combine with</span>
						{logicToggle}
					</div>
				)}
				{childrenEl}
				{addButtons}
			</div>
		);
	}

	return (
		<div className="well--accent">
			<div className="flex items-center justify-between mb-1.5">
				<div className="flex items-center gap-2">
					<span className="material-symbols-outlined icon-sm text-accent">
						account_tree
					</span>
					<span className="text-2xs font-semibold text-ink-700 uppercase tracking-wider">
						Group
					</span>
					{logicToggle}
				</div>
				{onRemove && (
					<button onClick={onRemove} className="icon-btn-inline" title="Remove group">
						<span className="material-symbols-outlined icon-sm">close</span>
					</button>
				)}
			</div>
			{childrenEl}
			{addButtons}
		</div>
	);
}
