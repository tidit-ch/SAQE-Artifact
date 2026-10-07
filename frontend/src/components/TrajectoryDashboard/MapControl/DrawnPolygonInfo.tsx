import { useState } from "react";

interface DrawnPolygonInfoProps {
	polygonWkt: string | null;
}

export default function DrawnPolygonInfo({
	polygonWkt,
}: DrawnPolygonInfoProps) {
	const [copied, setCopied] = useState(false);
	if (!polygonWkt) return null;

	const handleCopy = () => {
		navigator.clipboard.writeText(polygonWkt);
		setCopied(true);
		setTimeout(() => setCopied(false), 1200);
	};

	return (
		<div className="flex flex-col gap-1.5">
			<div className="flex items-center justify-between">
				<span className="hint">Polygon WKT</span>
				<button
					onClick={handleCopy}
					className="text-[10px] inline-flex items-center gap-0.5 text-ink-500 hover:text-accent"
				>
					<span className="material-symbols-outlined icon-sm">
						{copied ? "check" : "content_copy"}
					</span>
					{copied ? "copied" : "copy"}
				</button>
			</div>
			<pre className="code-block--light max-h-32 overflow-y-auto">
				{polygonWkt}
			</pre>
		</div>
	);
}
