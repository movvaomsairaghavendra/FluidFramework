/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { performance } from "@fluid-internal/client-utils";
import { ITelemetryBaseProperties } from "@fluidframework/core-interfaces";
import { assert, Deferred } from "@fluidframework/core-utils/internal";
import {
	IDeltasFetchResult,
	IStream,
	IStreamResult,
	ISequencedDocumentMessage,
} from "@fluidframework/driver-definitions/internal";
import {
	ITelemetryLoggerExt,
	PerformanceEvent,
} from "@fluidframework/telemetry-utils/internal";

import {
	canRetryOnError,
	createGenericNetworkError,
	getRetryDelayFromError,
} from "./network.js";
import { logNetworkFailure } from "./networkUtils.js";
// For now, this package is versioned and released in unison with the specific drivers
import { pkgVersion as driverVersion } from "./packageVersion.js";
import { calculateMaxWaitTime } from "./runWithRetry.js";

// We double this value in first try in when we calculate time to wait for in "calculateMaxWaitTime" function.
const MissingFetchDelayInMs = 50;

type WorkingState = "working" | "done" | "canceled";

/**
 * Helper class to organize parallel fetching of data
 * It can be used to concurrently do many requests, while consuming
 * data in the right order. Take a look at UT for examples.
 * @param concurrency - level of concurrency
 * @param from - starting point of fetching data (inclusive)
 * @param to - ending point of fetching data. exclusive, or undefined if unknown
 * @param payloadSize - batch size
 * @param logger - logger to use
 * @param requestCallback - callback to request batches
 * @returns Queue that can be used to retrieve data
 * @internal
 */
export class ParallelRequests<T> {
	private latestRequested: number;
	private nextToDeliver: number;
	private readonly results = new Map<number, T[]>();
	private workingState: WorkingState = "working";
	private requestsInFlight = 0;
	private readonly endEvent = new Deferred<void>();
	private requests = 0;
	private readonly knewTo: boolean;

	private get working(): boolean {
		return this.workingState === "working";
	}
	public get canceled(): boolean {
		return this.workingState === "canceled";
	}

	constructor(
		from: number,
		private to: number | undefined,
		private readonly payloadSize: number,
		private readonly logger: ITelemetryLoggerExt,
		private readonly requestCallback: (
			request: number,
			from: number,
			to: number,
			strongTo: boolean,
			props: ITelemetryBaseProperties,
		) => Promise<{ partial: boolean; cancel: boolean; payload: T[] }>,
		private readonly responseCallback: (payload: T[]) => void,
	) {
		this.latestRequested = from;
		this.nextToDeliver = from;
		this.knewTo = to !== undefined;
	}

	/**
	 * Cancels the parallel requests operation.
	 */
	public cancel(): void {
		if (this.working) {
			this.workingState = "canceled";
			this.endEvent.resolve();
		}
	}

	/**
	 * Runs the parallel requests with the specified level of concurrency.
	 * @param concurrency - The number of concurrent requests to make
	 * @returns A promise that resolves when all requests are complete
	 */
	public async run(concurrency: number): Promise<void> {
		assert(concurrency > 0, 0x102 /* "invalid level of concurrency" */);
		assert(this.working, 0x103 /* "trying to parallel run while not working" */);

		let c = concurrency;
		while (c > 0) {
			c--;
			this.addRequest();
		}
		this.dispatch(); // will recalculate and trigger this.endEvent if needed
		return this.endEvent.promise;
	}

	/**
	 * Marks the parallel requests operation as done.
	 */
	private done(): void {
		// We should satisfy request fully.
		assert(this.to !== undefined, 0x104 /* "undefined end point for parallel fetch" */);
		if (this.workingState === "working") {
			this.workingState = "done";
			this.endEvent.resolve();
		}
	}

	/**
	 * Marks the parallel requests operation as failed.
	 * @param error - The error that caused the failure
	 */
	private fail(error: Error): void {
		if (this.working) {
			this.workingState = "done";
			this.endEvent.reject(error);
		}
	}

	/**
	 * Dispatches the results in order and updates the state accordingly.
	 */
	private dispatch(): void {
		while (this.working) {
			const value = this.results.get(this.nextToDeliver);
			if (value === undefined) {
				break;
			}
			this.results.delete(this.nextToDeliver);
			assert(
				value.length <= this.payloadSize,
				0x1d9 /* "addRequestCore() should break into smaller chunks" */,
			);
			this.nextToDeliver += value.length;
			this.responseCallback(value);
		}

		// Account for cancellation - state might be not in consistent state on cancelling operation
		if (this.working) {
			if (this.requestsInFlight === 0) {
				// we should have dispatched everything, no matter whether we knew about the end or not.
				// see comment in addRequestCore() around throwing away chunk if it's above this.to
				assert(
					this.results.size === 0,
					0x107 /* "ending dispatch with remaining results to be sent" */,
				);
				this.done();
			} else if (this.to !== undefined && this.nextToDeliver >= this.to) {
				// Learned about the end and dispatched all the ops up to it.
				// Ignore all the in-flight requests above boundary - unblock caller sooner.
				assert(
					!this.knewTo,
					0x108 /* "ending results dispatch but knew in advance about more requests" */,
				);
				this.done();
			}
		}
	}

	/**
	 * Gets the next chunk of data to request.
	 * @returns The next chunk's from and to positions, or undefined if no more chunks
	 */
	private getNextChunk(): { from: number; to: number } | undefined {
		if (!this.working) {
			return undefined;
		}

		const from = this.latestRequested;
		if (this.to !== undefined && this.to <= from) {
			return undefined;
		}

		// this.latestRequested
		// inclusive on the right side! Exclusive on the left.
		this.latestRequested += this.payloadSize;

		if (this.to !== undefined) {
			this.latestRequested = Math.min(this.to, this.latestRequested);
		}

		assert(from < this.latestRequested, 0x109 /* "unexpected next chunk position" */);

		return { from, to: this.latestRequested };
	}

	/**
	 * Adds a new request to fetch data.
	 */
	private addRequest(): void {
		const chunk = this.getNextChunk();
		if (chunk === undefined) {
			return;
		}
		this.addRequestCore(chunk.from, chunk.to).catch(this.fail.bind(this));
	}

	/**
	 * Core implementation of adding a request to fetch data.
	 * @param fromArg - Starting position for the request
	 * @param toArg - Ending position for the request
	 * @returns A promise that resolves when the request is complete
	 */
	private async addRequestCore(fromArg: number, toArg: number): Promise<void> {
		assert(this.working, 0x10a /* "cannot add parallel request while not working" */);

		let from = fromArg;
		let to = toArg;

		// to & from are exclusive
		this.requestsInFlight++;
		while (this.working) {
			const requestedLength = to - from;
			assert(requestedLength > 0, 0x10b /* "invalid parallel request range" */);

			// We should not be wasting time asking for something useless.
			if (this.to !== undefined) {
				assert(from < this.to, 0x10c /* "invalid parallel request start point" */);
				assert(to <= this.to, 0x10d /* "invalid parallel request end point" */);
			}

			this.requests++;

			const promise = this.requestCallback(this.requests, from, to, this.to !== undefined, {});

			// dispatch any prior received data
			this.dispatch();

			const { payload, cancel, partial } = await promise;

			if (cancel) {
				this.cancel();
			}

			if (this.to !== undefined && from >= this.to) {
				// while we were waiting for response, we learned on what is the boundary
				// We can get here (with actual result!) if situation changed while this request was in
				// flight, i.e. the end was extended over what we learn in some other request
				// While it's useful not to throw this result, this is very corner cases and makes logic
				// (including consistency checks) much harder to write correctly.
				// So for now, we are throwing this result out the window.
				assert(
					!this.knewTo,
					0x10e /* "should not throw result if we knew about boundary in advance" */,
				);
				// Learn how often it happens and if it's too wasteful to throw these chunks.
				// If it pops into our view a lot, we would need to reconsider how we approach it.
				// Note that this is not visible to user other than potentially not hitting 100% of
				// what we can in perf domain.
				if (payload.length > 0) {
					this.logger.sendErrorEvent({
						eventName: "ParallelRequests_GotExtra",
						from,
						to,
						end: this.to,
						length: payload.length,
					});
				}

				break;
			}

			if (this.working) {
				const fromOrig = from;
				const length = payload.length;
				let fullChunk = requestedLength <= length; // we can possible get more than we asked.

				if (length === 0) {
					// 1. empty (partial) chunks should not be returned by various caching / adapter layers -
					//    they should fall back to next layer. This might be important invariant to hold to ensure
					//    that we are less likely have bugs where such layer would keep returning empty partial
					//    result on each call.
					// 2. Current invariant is that callback does retries until it gets something,
					//    with the goal of failing if zero data is retrieved in given amount of time.
					//    This is very specific property of storage / ops, so this logic is not here, but given only
					//    one user of this class, we assert that to catch issues earlier.
					// These invariant can be relaxed if needed.
					assert(
						!partial,
						0x10f /* "empty/partial chunks should not be returned by caching" */,
					);
					assert(
						!this.knewTo,
						0x110 /* "callback should retry until valid fetch before it learns new boundary" */,
					);
				} else {
					// We can get more than we asked for!
					// This can screw up logic in dispatch!
					// So push only batch size, and keep the rest for later - if conditions are favorable, we
					// will be able to use it. If not (parallel request overlapping these ops), it's easier to
					// discard them and wait for another (overlapping) request to come in later.
					if (requestedLength < length) {
						// This is error in a sense that it's not expected and likely points bug in other layer.
						// This layer copes with this situation just fine.
						this.logger.sendTelemetryEvent({
							eventName: "ParallelRequests_Over",
							from,
							to,
							length,
						});
					}
					const data = payload.splice(0, requestedLength);
					this.results.set(from, data);
					from += data.length;
				}

				if (!partial && !fullChunk) {
					if (!this.knewTo) {
						if (this.to === undefined || this.to > from) {
							// The END
							this.to = from;
						}
						break;
					}
					// We know that there are more items to be retrieved
					// Can we get partial chunk? Ideally storage indicates that's not a full chunk
					// Note that it's possible that not all ops hit storage yet.
					// We will come back to request more, and if we can't get any more ops soon, it's
					// catastrophic failure (see comment above on responsibility of callback to return something)
					// This layer will just keep trying until it gets full set.
					this.logger.sendPerformanceEvent({
						eventName: "ParallelRequests_Partial",
						from: fromOrig,
						to,
						length,
					});
				}

				if (to === this.latestRequested) {
					// we can go after full chunk at the end if we received partial chunk, or more than asked
					// Also if we got more than we asked to, we can actually use those ops!
					while (payload.length > 0) {
						const data = payload.splice(0, requestedLength);
						this.results.set(from, data);
						from += data.length;
					}

					this.latestRequested = from;
					fullChunk = true;
				}

				if (fullChunk) {
					const chunk = this.getNextChunk();
					if (chunk === undefined) {
						break;
					}
					from = chunk.from;
					to = chunk.to;
				}
			}
		}
		this.requestsInFlight--;
		this.dispatch();
	}
}

/**
 * Helper queue class to allow async push / pull
 * It's essentially a pipe allowing multiple writers, and single reader
 * @internal
 */
export class Queue<T> implements IStream<T> {
	private readonly queue: Promise<IStreamResult<T>>[] = [];
	private deferred: Deferred<IStreamResult<T>> | undefined;
	private done = false;

	/**
	 * Pushes a value to the queue.
	 * @param value - The value to push
	 */
	public pushValue(value: T): void {
		this.pushCore(Promise.resolve({ done: false, value }));
	}

	/**
	 * Pushes an error to the queue.
	 * @param error - The error to push
	 */
	public pushError(error: Error): void {
		this.pushCore(Promise.reject(error));
	}

	/**
	 * Marks the queue as done.
	 */
	public pushDone(): void {
		this.done = true;
		this.pushCore(Promise.resolve({ done: true }));
	}

	/**
	 * Core implementation of pushing a value to the queue.
	 * @param value - The value to push
	 */
	protected pushCore(value: Promise<IStreamResult<T>>): void {
		if (this.deferred) {
			this.deferred.resolve(value);
			this.deferred = undefined;
		} else {
			this.queue.push(value);
		}
	}

	/**
	 * Reads the next value from the queue.
	 * @returns A promise that resolves with the next value
	 */
	public async read(): Promise<IStreamResult<T>> {
		const value = this.queue.shift();
		if (value !== undefined) {
			return value;
		}
		if (this.done) {
			return { done: true };
		}
		this.deferred = new Deferred<IStreamResult<T>>();
		return this.deferred.promise;
	}
}

/**
 * Waits for the browser to be online.
 * @returns A promise that resolves when the browser is online
 */
const waitForOnline = async (): Promise<void> => {
	// Only wait if we have a strong signal that we're offline - otherwise assume we're online.
	if (typeof navigator === "undefined") {
		return;
	}
	if (navigator.onLine) {
		return;
	}

	return new Promise<void>((resolve) => {
		const onOnline = (): void => {
			window.removeEventListener("online", onOnline);
			resolve();
		};
		window.addEventListener("online", onOnline);
	});
};

/**
 * Gets a single batch of operations.
 * @param get - Function to get the operations
 * @param props - Telemetry properties
 * @param strongTo - Whether the 'to' parameter is strongly defined
 * @param logger - Logger to use
 * @param signal - Optional abort signal
 * @param scenarioName - Optional scenario name
 * @returns A promise that resolves with the batch result
 */
async function getSingleOpBatch(
	get: (telemetryProps: ITelemetryBaseProperties) => Promise<IDeltasFetchResult>,
	props: ITelemetryBaseProperties,
	strongTo: boolean,
	logger: ITelemetryLoggerExt,
	signal?: AbortSignal,
	scenarioName?: string,
): Promise<{ partial: boolean; cancel: boolean; payload: ISequencedDocumentMessage[] }> {
	let result: IDeltasFetchResult | undefined;
	let waitTime = MissingFetchDelayInMs;
	let retries = 0;
	let lastSuccessTime: number | undefined;
	let telemetryEvent: PerformanceEvent | undefined;

	while (signal?.aborted === false) {
		try {
			result = await get(props);
			break;
		} catch (error) {
			const typedError = error as { canRetry?: boolean; retryAfterSeconds?: number };
			if (!canRetryOnError(typedError)) {
				throw error;
			}
			const retryAfterMs = getRetryDelayFromError(typedError);
			waitTime = retryAfterMs ?? calculateMaxWaitTime(waitTime, retries);
			retries++;
			logNetworkFailure(
				logger,
				{
					eventName: "OpsFetchError",
					retries,
					waitTime,
					scenarioName,
				},
				typedError,
			);

			if (telemetryEvent === undefined) {
				telemetryEvent = PerformanceEvent.start(logger, {
					eventName: "GetDeltasWaitTime",
				});
			}

			await waitForOnline();
			await new Promise((resolve) => setTimeout(resolve, waitTime));
		}
	}

	if (result === undefined) {
		return { payload: [], cancel: true, partial: false };
	}

	// If we got messages back, return them. Return regardless of whether we got messages back if we didn't
	// specify a "to", since we don't have an expectation of how many to receive.
	const hasMessages = result.messages.length > 0;
	const shouldReturn = hasMessages || !strongTo;

	// Report this event if we waited to fetch ops due to being offline or throttling.
	if (telemetryEvent !== undefined) {
		telemetryEvent.end({
			...props,
			reason: scenarioName,
		});
	}

	if (shouldReturn) {
		return { payload: result.messages, cancel: false, partial: result.partialResult };
	}

	// Otherwise, the storage gave us back an empty set of ops but we were expecting a non-empty set.
	if (lastSuccessTime === undefined) {
		// Take timestamp of the first time server responded successfully, even though it wasn't with the ops we asked for.
		// If we keep getting empty responses we'll eventually fail out below.
		lastSuccessTime = performance.now();
	} else if (performance.now() - lastSuccessTime > 30000) {
		// If we are connected and receiving proper responses from server, but can't get any ops back,
		// then give up after some time. This likely indicates the issue with ordering service not flushing
		throw createGenericNetworkError(
			"Unable to get ops - server is not providing ops in time",
			{ canRetry: false },
			{
				retries,
				driverVersion,
				...props,
			},
		);
	}

	return { payload: [], cancel: true, partial: false };
}

/**
 * Function to request operations in parallel.
 * @param get - Function to get the operations
 * @param concurrency - Number of concurrent requests
 * @param fromTotal - Starting position
 * @param toTotal - Ending position (optional)
 * @param payloadSize - Size of each batch
 * @param logger - Logger to use
 * @param signal - Optional abort signal
 * @param scenarioName - Optional scenario name
 * @returns A stream of operation batches
 * @internal
 */
export function requestOps(
	get: (
		from: number,
		to: number,
		telemetryProps: ITelemetryBaseProperties,
	) => Promise<IDeltasFetchResult>,
	concurrency: number,
	fromTotal: number,
	toTotal: number | undefined,
	payloadSize: number,
	logger: ITelemetryLoggerExt,
	signal?: AbortSignal,
	scenarioName?: string,
): IStream<ISequencedDocumentMessage[]> {
	let requests = 0;
	let lastFetch: number | undefined;
	let length = 0;
	const queue = new Queue<ISequencedDocumentMessage[]>();

	const propsTotal: ITelemetryBaseProperties = {
		fromTotal,
		toTotal,
	};

	const telemetryEvent = PerformanceEvent.start(logger, {
		eventName: "GetDeltas",
		...propsTotal,
		reason: scenarioName,
	});

	const manager = new ParallelRequests<ISequencedDocumentMessage>(
		fromTotal,
		toTotal,
		payloadSize,
		logger,
		async (
			request: number,
			from: number,
			to: number,
			strongTo: boolean,
			propsPerRequest: ITelemetryBaseProperties,
		): Promise<{
			partial: boolean;
			cancel: boolean;
			payload: ISequencedDocumentMessage[];
		}> => {
			requests++;
			return getSingleOpBatch(
				async (propsAll) => get(from, to, propsAll),
				{ request, from, to, ...propsTotal, ...propsPerRequest },
				strongTo,
				logger,
				signal,
				scenarioName,
			);
		},
		(deltas: ISequencedDocumentMessage[]): void => {
			// Assert continuing and right start.
			if (lastFetch === undefined) {
				assert(deltas[0].sequenceNumber === fromTotal, 0x26d /* "wrong start" */);
			} else {
				assert(deltas[0].sequenceNumber === lastFetch + 1, 0x26e /* "wrong start" */);
			}
			lastFetch = deltas[deltas.length - 1].sequenceNumber;
			assert(
				lastFetch - deltas[0].sequenceNumber + 1 === deltas.length,
				0x26f /* "continuous and no duplicates" */,
			);
			length += deltas.length;
			queue.pushValue(deltas);
		},
	);

	// Implement faster cancellation. getSingleOpBatch() checks signal, but only in between
	// waits (up to 10 seconds) and fetches (can take infinite amount of time).
	// While every such case should be improved and take into account signal (and thus cancel immediately),
	// it is beneficial to have catch-all
	const listener = (event: Event): void => {
		manager.cancel();
	};
	if (signal !== undefined) {
		signal.addEventListener("abort", listener);
	}

	manager
		.run(concurrency)
		.finally(() => {
			if (signal !== undefined) {
				signal.removeEventListener("abort", listener);
			}
		})
		.then(() => {
			const props = {
				lastFetch,
				length,
				requests,
			};
			if (manager.canceled) {
				telemetryEvent.cancel({ ...props, error: "ops request cancelled by client" });
			} else {
				assert(
					toTotal === undefined || (lastFetch !== undefined && lastFetch >= toTotal - 1),
					0x270 /* "All requested ops fetched" */,
				);
				telemetryEvent.end(props);
			}
			queue.pushDone();
		})
		.catch((error: unknown) => {
			telemetryEvent.cancel(
				{
					lastFetch,
					length,
					requests,
				},
				error instanceof Error ? error : new Error(String(error)),
			);
			queue.pushError(error instanceof Error ? error : new Error(String(error)));
		});

	return queue;
}

/**
 * @internal
 */
export const emptyMessageStream: IStream<ISequencedDocumentMessage[]> = {
	read: async () => {
		return { done: true };
	},
};

/**
 * Function to create a stream from a promise of messages.
 * @param messagesArg - Promise of messages
 * @returns A stream of messages
 * @internal
 */
export function streamFromMessages(
	messagesArg: Promise<ISequencedDocumentMessage[]>,
): IStream<ISequencedDocumentMessage[]> {
	const queue = new Queue<ISequencedDocumentMessage[]>();
	const onFulfilled = (messages: ISequencedDocumentMessage[]): void => {
		queue.pushValue(messages);
		queue.pushDone();
	};
	const onRejected = (error: unknown): void => {
		const errorToUse = error instanceof Error ? error : new Error(String(error));
		queue.pushError(errorToUse);
	};
	messagesArg.then(onFulfilled, onRejected).catch(() => {
		// Ignore errors in promise handling
	});
	return queue;
}

/**
 * Function to create a stream observer that calls a handler for each value.
 * @param stream - The stream to observe
 * @param handler - The handler to call for each value
 * @returns The original stream
 * @internal
 */
export function streamObserver<T>(
	stream: IStream<T>,
	handler: (value: IStreamResult<T>) => void,
): IStream<T> {
	const read = async (): Promise<void> => {
		try {
			const value = await stream.read();
			handler(value);
			if (value.done) {
				return;
			}
			read().catch(() => {
				// Ignore errors in recursive read
			});
		} catch {
			handler({ done: false, value: undefined as unknown as T });
		}
	};
	read().catch(() => {
		// Ignore errors in initial read
	});
	return stream;
}
