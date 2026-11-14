/**
 * Returns a trimmed environment variable value, treating unresolved
 * Zerops `${VAR}` placeholders as unset.
 */
export function cleanEnv(value: string | undefined, fallback = ""): string {
  const trimmed = (value || "").trim();
  if (!trimmed || (trimmed.startsWith("${") && trimmed.endsWith("}"))) {
    return fallback;
  }
  return trimmed;
}
