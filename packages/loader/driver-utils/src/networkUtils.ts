/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import {
	ITelemetryErrorEventExt,
	ITelemetryLoggerExt,
	TelemetryEventCategory,
	ITelemetryGenericEventExt,
} from "@fluidframework/telemetry-utils/internal";

import { OnlineStatus, canRetryOnError, isOnline } from "./network.js";

/**
 * @internal
 */
interface NavigatorWithConnection extends Navigator {
	connection?: { type?: string };
	mozConnection?: { type?: string };
	webkitConnection?: { type?: string };
}

/**
 * @internal
 */
interface INetworkEvent extends ITelemetryGenericEventExt {
	online?: string;
	connectionType?: string;
	category?: TelemetryEventCategory;
}

/**
 * Logs network failure events with additional connection information.
 * Includes online status and connection type in the telemetry event.
 * @internal
 */
export function logNetworkFailure(
	logger: ITelemetryLoggerExt,
	event: ITelemetryErrorEventExt,
	error?: Error | { canRetry?: boolean } | unknown,
): void {
	const newEvent: INetworkEvent = {
		...event,
		eventName: event.eventName,
		online: undefined,
		connectionType: undefined,
		category: undefined,
	};

	const errorOnlineProp = (error as { online?: string })?.online;
	newEvent.online =
		typeof errorOnlineProp === "string" ? errorOnlineProp : OnlineStatus[isOnline()];

	if (typeof navigator === "object" && navigator !== null) {
		const nav = navigator as NavigatorWithConnection;
		const connection = nav.connection ?? nav.mozConnection ?? nav.webkitConnection;
		if (connection !== null && typeof connection === "object") {
			newEvent.connectionType = connection.type;
		}
	}

	// non-retryable errors are fatal and should be logged as errors
	newEvent.category = canRetryOnError(error as { canRetry?: boolean }) ? "generic" : "error";
	logger.sendTelemetryEvent(newEvent, error);
}

/**
 * Gets the current connection information including connection type and online status.
 * Uses browser's connection API to determine connection type and online status.
 * @returns Object containing connection type and online status
 * @internal
 */
export function getConnectionInfo(): { type: string | undefined; online: boolean } {
	const online = isOnline() === OnlineStatus.Online;
	let connection: { type?: string } | undefined;
	if (typeof navigator === "object" && navigator !== null) {
		const nav = navigator as NavigatorWithConnection;
		connection = nav.connection ?? nav.mozConnection ?? nav.webkitConnection;
	}
	const type = connection?.type;
	return { type, online };
}
