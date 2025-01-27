/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { performance } from "@fluid-internal/client-utils";
import { delay } from "@fluidframework/core-utils/internal";
import { DriverErrorTypes } from "@fluidframework/driver-definitions/internal";
import { ITelemetryLoggerExt, isFluidError } from "@fluidframework/telemetry-utils/internal";

import { NonRetryableError, canRetryOnError, getRetryDelayFromError } from "./network.js";
import { pkgVersion } from "./packageVersion.js";

/**
 * Interface for retryable errors that can be handled by the retry logic.
 * @public
 */
export interface IRetryableError {
	canRetry?: boolean;
	retryAfterSeconds?: number;
}

/**
 * Interface for tracking progress and controlling retry operations.
 * @internal
 */
export interface IProgress {
	/**
	 * Abort signal used to cancel operation.
	 *
	 * @remarks Note that most of the layers do not use this signal yet. We need to change that over time.
	 * Please consult with API documentation / implementation.
	 * Note that  number of layers may not check this signal while holding this request in a queue,
	 * so it may take a while it takes effect. This can be improved in the future.
	 *
	 * The layers in question are:
	 *
	 * - driver (RateLimiter)
	 *
	 * - runWithRetry
	 */
	cancel?: AbortSignal;

	/**
	 * Called whenever api returns cancellable error and the call is going to be retried.
	 * Any exception thrown from this call back result in cancellation of operation
	 * and propagation of thrown exception.
	 * @param delayInMs - delay before next retry. This value will depend on internal back-off logic,
	 * as well as information provided by service (like 429 error asking to wait for some time before retry)
	 * @param error - error object returned from the call.
	 */
	onRetry?(delayInMs: number, error: IRetryableError): void;

	reportProgress?: (progress: number) => void;
}

interface ITelemetryProps {
	eventName: string;
	retry: number;
	duration: number;
	fetchCallName: string;
	reason?: string;
	[key: string]: string | number | undefined;
}

/**
 * Runs an API call with retry logic for handling transient failures.
 * Implements exponential backoff and respects server-provided retry delays.
 * @param api - The API function to call that returns a promise
 * @param fetchCallName - Name of the fetch call for telemetry
 * @param logger - Logger for telemetry events
 * @param progress - Progress object for cancellation and retry notifications
 * @returns Promise that resolves with the API result or rejects with an error
 * @internal
 */
export async function runWithRetry<T>(
	api: (cancel?: AbortSignal) => Promise<T>,
	fetchCallName: string,
	logger: ITelemetryLoggerExt,
	progress: IProgress,
): Promise<T> {
	let result!: T;
	let success = false;
	let retryAfterMs = 500;
	let numRetries = 0;
	const startTime = performance.now();

	do {
		try {
			result = await api(progress.cancel);
			success = true;
		} catch (error: unknown) {
			// Cast error to RetryableError type with required properties
			const errorWithRetry =
				typeof error === "object" && error !== null ? (error as { canRetry?: boolean }) : {};
			const typedError: IRetryableError = {
				canRetry: canRetryOnError(errorWithRetry),
				retryAfterSeconds:
					typeof error === "object" && error !== null && "retryAfterSeconds" in error
						? (error as { retryAfterSeconds?: number }).retryAfterSeconds
						: undefined,
			};

			// If it is not retriable, then just throw the error.
			if (typedError.canRetry === false) {
				const errorToLog = error instanceof Error ? error : new Error(String(error));
				const telemetryProps: ITelemetryProps = {
					eventName: `${fetchCallName}_cancel`,
					retry: numRetries,
					duration: performance.now() - startTime,
					fetchCallName,
				};
				logger.sendTelemetryEvent(telemetryProps, errorToLog);
				throw errorToLog;
			}

			if (progress.cancel?.aborted === true) {
				const errorToLog = error instanceof Error ? error : new Error(String(error));
				const telemetryProps: ITelemetryProps = {
					eventName: `${fetchCallName}_runWithRetryAborted`,
					retry: numRetries,
					duration: performance.now() - startTime,
					fetchCallName,
					reason: progress.cancel.reason as string,
				};
				logger.sendTelemetryEvent(telemetryProps, errorToLog);
				throw new NonRetryableError(
					"runWithRetry was Aborted",
					DriverErrorTypes.genericError,
					{
						driverVersion: pkgVersion,
						fetchCallName,
						reason: progress.cancel.reason as string,
					},
				);
			}

			const retryDelay = getRetryDelayFromError(typedError);
			if (retryDelay !== undefined) {
				retryAfterMs = retryDelay;
			}

			if (progress.onRetry) {
				progress.onRetry(retryAfterMs, typedError);
			}

			await delay(retryAfterMs);
			numRetries++;
		}
	} while (!success);

	return result;
}

const MaxReconnectDelayInMsWhenEndpointIsReachable = 60000;
const MaxReconnectDelayInMsWhenEndpointIsNotReachable = 8000;

/**
 * Calculates time to wait for after an error based on the error and wait time for previous iteration.
 * In case endpoint(service or socket) is not reachable, then we maybe offline or may have got some
 * transient error not related to endpoint, in that case we want to try at faster pace and hence the
 * max wait is lesser 8s as compared to when endpoint is reachable in which case it is 60s.
 * @param delayMs - wait time for previous iteration
 * @param error - error based on which we decide wait time.
 * @returns Wait time to wait for.
 * @internal
 */
export function calculateMaxWaitTime(delayMs: number, error: unknown): number {
	const typedError =
		typeof error === "object" && error !== null && "retryAfterSeconds" in error
			? (error as { retryAfterSeconds?: number })
			: { retryAfterSeconds: undefined };

	const retryDelayFromError = getRetryDelayFromError(typedError);
	let newDelayMs = Math.max(retryDelayFromError ?? 0, delayMs * 2);
	newDelayMs = Math.min(
		newDelayMs,
		isFluidError(error) && error.getTelemetryProperties().endpointReached === true
			? MaxReconnectDelayInMsWhenEndpointIsReachable
			: MaxReconnectDelayInMsWhenEndpointIsNotReachable,
	);
	return newDelayMs;
}

/**
 * Callback function that is called when a retry operation occurs.
 * @public
 */
export const onRetry = (
	error: IRetryableError,
	retries: number,
	waitTime: number,
	logger?: ITelemetryLoggerExt,
	scenarioName?: string,
): void => {
	// Implementation of the onRetry function
};
