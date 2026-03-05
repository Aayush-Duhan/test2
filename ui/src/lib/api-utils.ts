import { NextResponse } from "next/server";

/**
 * Error payload structure from Python backend
 */
interface PythonErrorResponse {
  detail?: unknown;
  error?: unknown;
}

/**
 * Extracts error message from Python backend response
 */
async function extractErrorMessage(response: Response): Promise<string> {
  try {
    const payload = (await response.json()) as PythonErrorResponse;
    const candidate = payload?.detail ?? payload?.error;
    if (typeof candidate === "string") return candidate;
    if (candidate == null) return "Operation failed";
    return JSON.stringify(candidate);
  } catch {
    return "Operation failed";
  }
}

/**
 * Handles Python backend responses uniformly.
 * On success, passes response data to the success handler.
 * On failure, returns appropriate error response.
 *
 * @param response - Fetch response from Python backend
 * @param successHandler - Transforms successful response data
 * @param errorMessage - Fallback error message
 * @returns NextResponse with either success data or error
 */
export async function handlePythonResponse<TInput = unknown, TOutput = unknown>(
  response: Response,
  successHandler: (data: TInput) => TOutput,
  errorMessage = "Operation failed"
): Promise<NextResponse> {
  if (!response.ok) {
    const message = await extractErrorMessage(response);
    const finalMessage = message === "Operation failed" ? errorMessage : message;
    const status = response.status >= 500 ? 503 : response.status;
    return NextResponse.json({ error: finalMessage }, { status });
  }

  const data = await response.json() as TInput;
  return NextResponse.json(successHandler(data));
}

/**
 * Handles API errors uniformly, extracting message from Error instances.
 *
 * @param error - Caught error from try block
 * @param fallbackMessage - Message to use if error isn't an Error instance
 * @returns NextResponse with error and 503 status
 */
export function handleApiError(error: unknown, fallbackMessage: string): NextResponse {
  const message = error instanceof Error ? error.message : fallbackMessage;
  return NextResponse.json({ error: message }, { status: 503 });
}

/**
 * Wraps an API handler with uniform error handling.
 * Useful for simple endpoints that just proxy to Python backend.
 *
 * @param handler - Async function that may throw
 * @param errorMessage - Fallback error message
 * @returns NextResponse with either handler result or error
 */
export async function withErrorHandling(
  handler: () => Promise<Response>,
  errorMessage: string
): Promise<Response> {
  try {
    return await handler();
  } catch (error) {
    return handleApiError(error, errorMessage);
  }
}

/**
 * Validates required FormData file field.
 *
 * @param formData - FormData object to extract from
 * @param fieldName - Name of the file field
 * @param errorMessage - Error message if file is missing
 * @returns Tuple of [file, null] on success or [null, NextResponse] on error
 */
export function requireFormDataFile(
  formData: FormData,
  fieldName: string,
  errorMessage = "File is required"
): [File, null] | [null, NextResponse] {
  const file = formData.get(fieldName);
  if (!file || !(file instanceof File)) {
    return [null, NextResponse.json({ error: errorMessage }, { status: 400 })];
  }
  return [file, null];
}

/**
 * Parses a numeric form field with fallback.
 *
 * @param formData - FormData object to extract from
 * @param fieldName - Name of the field
 * @param fallback - Value to return if field is missing or invalid
 * @returns Parsed number or fallback
 */
export function parseFormNumber(
  formData: FormData,
  fieldName: string,
  fallback: number
): number {
  const raw = formData.get(fieldName);
  if (typeof raw !== "string") return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * Parses a JSON array form field with fallback.
 *
 * @param formData - FormData object to extract from
 * @param fieldName - Name of the field
 * @returns Parsed array or empty array on error
 */
export function parseFormJsonArray<T>(
  formData: FormData,
  fieldName: string
): T[] {
  const raw = formData.get(fieldName);
  if (typeof raw !== "string" || !raw.trim()) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/**
 * Parses a string form field with fallback.
 *
 * @param formData - FormData object to extract from
 * @param fieldName - Name of the field
 * @param fallback - Value to return if field is missing or empty
 * @returns String value or fallback
 */
export function parseFormString(
  formData: FormData,
  fieldName: string,
  fallback: string
): string {
  const raw = formData.get(fieldName);
  return typeof raw === "string" && raw.trim().length > 0 ? raw.trim() : fallback;
}
