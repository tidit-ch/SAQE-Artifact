import { useMemo } from "react";
import type { TrajectoryGroupState } from "../../../hooks/useTrajectoryList";

interface RawResultsProps {
	groups: TrajectoryGroupState[];
}

export default function RawResults({ groups }: RawResultsProps) {
	const raw = useMemo(
		() => groups.flatMap((g) => g.items.map((i) => i.entity)),
		[groups],
	);

	if (raw.length === 0) {
		return <p className="hint text-center py-2">No results yet.</p>;
	}

	const json = JSON.stringify(raw, null, 2);

	return (
		<div className="relative group">
			<button
				onClick={() => navigator.clipboard.writeText(json)}
				className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity text-[10px] text-ink-500 hover:text-ink-900 inline-flex items-center gap-0.5 bg-white/80 rounded px-1 py-0.5"
				title="Copy JSON"
			>
				<span className="material-symbols-outlined icon-sm">content_copy</span>
				copy
			</button>
			<pre className="code-block max-h-96 overflow-auto text-[11px] leading-relaxed">
				{json}
			</pre>
		</div>
	);
}
