import { useMemo } from "react";
import { DateTimeRange } from "../../../hooks/useTrajectoryList";

interface DateTimeFilterProps {
	dateTimeRange: DateTimeRange | null;
}

function formatDateTimeForInput(date: Date): string {
	const pad = (n: number) => String(n).padStart(2, "0");
	return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export default function DateTimeFilter({ dateTimeRange }: DateTimeFilterProps) {
	const { minStr, maxStr, minLimitStr, maxLimitStr } = useMemo(() => {
		if (!dateTimeRange)
			return { minStr: "", maxStr: "", minLimitStr: "", maxLimitStr: "" };
		const minLimit = new Date(dateTimeRange.min.getTime() - 60000);
		const maxLimit = new Date(dateTimeRange.max.getTime() + 60000);
		return {
			minStr: formatDateTimeForInput(dateTimeRange.min),
			maxStr: formatDateTimeForInput(dateTimeRange.max),
			minLimitStr: formatDateTimeForInput(minLimit),
			maxLimitStr: formatDateTimeForInput(maxLimit),
		};
	}, [dateTimeRange]);

	if (!dateTimeRange) {
		return (
			<p className="hint italic">
				Run a query to see the available time range.
			</p>
		);
	}

	return (
		<form
			onSubmit={(e) => e.preventDefault()}
			className="flex flex-col gap-2"
		>
			<div className="grid grid-cols-2 gap-2">
				<div className="flex flex-col gap-1">
					<label className="field-label">Start</label>
					<input
						type="datetime-local"
						defaultValue={minStr}
						min={minLimitStr}
						max={maxLimitStr}
						className="input"
					/>
				</div>
				<div className="flex flex-col gap-1">
					<label className="field-label">End</label>
					<input
						type="datetime-local"
						defaultValue={maxStr}
						min={minLimitStr}
						max={maxLimitStr}
						className="input"
					/>
				</div>
			</div>
			<button type="submit" className="btn btn-secondary btn-sm self-end">
				<span className="material-symbols-outlined icon-sm">filter_alt</span>
				Apply filter
			</button>
		</form>
	);
}
