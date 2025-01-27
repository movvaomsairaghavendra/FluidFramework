/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { strict as assert } from "node:assert";

import { DriverErrorTypes } from "@fluidframework/driver-definitions/internal";
import { createChildLogger } from "@fluidframework/telemetry-utils/internal";

import { runWithRetry } from "../runWithRetry.js";

const _setTimeout = global.setTimeout;
const fastSetTimeout = (
	callback: (...cbArgs: unknown[]) => void,
	ms: number,
	...args: unknown[]
): NodeJS.Timeout => _setTimeout(callback, ms / 1000, ...args);

async function runWithFastSetTimeout<T>(callback: () => Promise<T>): Promise<T> {
	global.setTimeout = fastSetTimeout as typeof global.setTimeout;
	return callback().finally(() => {
		global.setTimeout = _setTimeout;
	});
}

describe("runWithRetry Tests", () => {
	const logger = createChildLogger();

	it("Should succeed at first time", async (): Promise<void> => {
		let retryTimes = 1;
		let success = false;
		const api = async (): Promise<boolean> => {
			retryTimes -= 1;
			return true;
		};

		let emitDelayInfoTimes = 0;
		success = await runWithFastSetTimeout(async () =>
			runWithRetry(api, "test", logger, {
				onRetry: (): void => {
					emitDelayInfoTimes += 1;
				},
			}),
		);
		assert.strictEqual(retryTimes, 0, "Should succeed at first time");
		assert.strictEqual(success, true, "Retry should succeed ultimately");
		assert.strictEqual(emitDelayInfoTimes, 0, "Should not emit delay at first time");
	});

	it("Check that it retries infinitely", async (): Promise<void> => {
		const maxTries = 5;
		let retryTimes = maxTries;
		let success = false;
		const api = async (): Promise<boolean> => {
			if (retryTimes > 0) {
				retryTimes -= 1;
				const error = new Error("Throw error") as Error & {
					retryAfterSeconds?: number;
					canRetry?: boolean;
				};
				error.retryAfterSeconds = 10;
				error.canRetry = true;
				throw error;
			}
			return true;
		};

		let emitDelayInfoTimes = 0;
		success = await runWithFastSetTimeout(async () =>
			runWithRetry(api, "test", logger, {
				onRetry: (): void => {
					emitDelayInfoTimes += 1;
				},
			}),
		);
		assert.strictEqual(retryTimes, 0, "Should keep retrying until success");
		assert.strictEqual(success, true, "Retry should succeed ultimately");
		assert.strictEqual(emitDelayInfoTimes, maxTries, "Should emit delay at each try");
	});

	it("Check that it retries after retry seconds", async (): Promise<void> => {
		let retryTimes = 1;
		let success = false;
		const api = async (): Promise<boolean> => {
			if (retryTimes > 0) {
				retryTimes -= 1;
				const error = new Error("Throttle Error") as Error & {
					errorType?: string;
					retryAfterSeconds?: number;
					canRetry?: boolean;
				};
				error.errorType = DriverErrorTypes.throttlingError;
				error.retryAfterSeconds = 400;
				error.canRetry = true;
				throw error;
			}
			return true;
		};
		success = await runWithFastSetTimeout(async () => runWithRetry(api, "test", logger, {}));
		assert.strictEqual(retryTimes, 0, "Should retry once");
		assert.strictEqual(success, true, "Retry should succeed ultimately");
	});

	it("If error is just a string, should retry as canRetry is not false", async (): Promise<void> => {
		let retryTimes = 1;
		let success = false;
		const api = async (): Promise<boolean> => {
			if (retryTimes > 0) {
				retryTimes -= 1;
				const err = new Error("error") as Error & { canRetry?: boolean };
				err.canRetry = true;
				throw err;
			}
			return true;
		};
		try {
			success = await runWithFastSetTimeout(async () => runWithRetry(api, "test", logger, {}));
		} catch {
			// Expected error - test verifies retry behavior
		}
		assert.strictEqual(retryTimes, 0, "Should retry");
		assert.strictEqual(success, true, "Should succeed as retry should be successful");
	});

	it("Should not retry if canRetry is set as false", async (): Promise<void> => {
		let retryTimes = 1;
		let success = false;
		const api = async (): Promise<boolean> => {
			if (retryTimes > 0) {
				retryTimes -= 1;
				const error = new Error("error") as Error & { canRetry?: boolean };
				error.canRetry = false;
				throw error;
			}
			return true;
		};
		try {
			success = await runWithFastSetTimeout(async () => runWithRetry(api, "test", logger, {}));
			assert.fail("Should not succeed");
		} catch {
			// Expected error - test verifies no retry on canRetry=false
		}
		assert.strictEqual(retryTimes, 0, "Should not retry");
		assert.strictEqual(success, false, "Should not succeed as canRetry was not set");
	});

	it("Should not retry if canRetry is not set", async (): Promise<void> => {
		let retryTimes = 1;
		let success = false;
		const api = async (): Promise<boolean> => {
			if (retryTimes > 0) {
				retryTimes -= 1;
				const error = new Error("error");
				throw error;
			}
			return true;
		};
		try {
			success = await runWithFastSetTimeout(async () => runWithRetry(api, "test", logger, {}));
			assert.fail("Should not succeed");
		} catch {
			// Expected error - test verifies no retry when canRetry is not set
		}
		assert.strictEqual(retryTimes, 0, "Should not retry");
		assert.strictEqual(success, false, "Should not succeed as canRetry was not set");
	});

	it("Should not retry if it is disabled", async (): Promise<void> => {
		let retryTimes = 1;
		let success = false;
		const api = async (): Promise<boolean> => {
			if (retryTimes > 0) {
				retryTimes -= 1;
				const error = new Error("error") as Error & { canRetry?: boolean };
				error.canRetry = true;
				throw error;
			}
			return true;
		};
		try {
			success = await runWithFastSetTimeout(async () =>
				runWithRetry(api, "test", logger, {
					onRetry: (): void => {
						throw new Error("disposed");
					},
				}),
			);
			assert.fail("Should not succeed");
		} catch {
			// Expected error - test verifies no retry when disabled
		}
		assert.strictEqual(retryTimes, 0, "Should not retry");
		assert.strictEqual(success, false, "Should not succeed as retrying was disabled");
	});

	it("Abort reason is included in thrown exception", async (): Promise<void> => {
		const abortController = new AbortController();

		const api = async (): Promise<never> => {
			abortController.abort("Sample abort reason");
			const error = new Error("aborted") as Error & { canRetry?: boolean };
			error.canRetry = true;
			throw error;
		};
		try {
			await runWithFastSetTimeout(async () =>
				runWithRetry(api, "test", logger, {
					cancel: abortController.signal,
				}),
			);
			assert.fail("Should not succeed");
		} catch (error) {
			const typedError = error as { message: string; reason?: string };
			assert.strictEqual(typedError.message, "runWithRetry was Aborted");
			assert.strictEqual(typedError.reason, "Sample abort reason");
		}
	});
});
