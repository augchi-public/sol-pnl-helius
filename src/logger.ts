const DEBUG_ENABLED = /^(1|true|yes|on)$/i.test(
  process.env.SOL_PNL_DEBUG ?? "",
);
const START_MS = performance.now();

function safeSerialize(value: unknown): string {
  try {
    const json = JSON.stringify(value);
    if (!json) return "";
    if (json.length > 2000) {
      return json.slice(0, 2000) + "...";
    }
    return json;
  } catch {
    return '"[unserializable]"';
  }
}

export function isDebugEnabled(): boolean {
  return DEBUG_ENABLED;
}

export function debugLog(
  scope: string,
  message: string,
  details?: Record<string, unknown>,
): void {
  if (!DEBUG_ENABLED) return;
  const iso = new Date().toISOString();
  const elapsed = (performance.now() - START_MS).toFixed(1);
  const suffix = details ? ` ${safeSerialize(details)}` : "";
  console.log(`[debug ${iso} +${elapsed}ms] [${scope}] ${message}${suffix}`);
}
