/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import type {
	IDocumentStorageServicePolicies,
	ISnapshotTree,
	IVersion,
} from "@fluidframework/driver-definitions/internal";
import { LoaderCachingPolicy } from "@fluidframework/driver-definitions/internal";

import { DocumentStorageServiceProxy } from "./documentStorageServiceProxy.js";
import { canRetryOnError } from "./network.js";

/**
 * @internal
 */
export class PrefetchDocumentStorageService extends DocumentStorageServiceProxy {
	// BlobId -> blob prefetchCache cache
	private readonly prefetchCache = new Map<string, Promise<ArrayBufferLike>>();
	private prefetchEnabled = true;

	public get policies(): IDocumentStorageServicePolicies | undefined {
		const policies = this.internalStorageService.policies;
		if (policies) {
			return { ...policies, caching: LoaderCachingPolicy.NoCaching };
		}
	}

	/* eslint-disable-next-line @rushstack/no-new-null */
	public async getSnapshotTree(version?: IVersion): Promise<ISnapshotTree | null> {
		const p = this.internalStorageService.getSnapshotTree(version);
		if (this.prefetchEnabled) {
			// We don't care if the prefetch succeeds
			p.then((result) => {
				if (result !== undefined && result !== null) {
					this.prefetchTree(result);
				}
			}).catch(() => undefined);
		}
		const tree = await p;
		/* eslint-disable-next-line unicorn/no-null */
		return tree ?? null;
	}

	public async readBlob(blobId: string): Promise<ArrayBufferLike> {
		return this.cachedRead(blobId);
	}

	public stopPrefetch(): void {
		this.prefetchEnabled = false;
		this.prefetchCache.clear();
	}

	private async cachedRead(blobId: string): Promise<ArrayBufferLike> {
		if (this.prefetchEnabled) {
			const prefetchedBlobP = this.prefetchCache.get(blobId);
			if (prefetchedBlobP !== undefined) {
				return prefetchedBlobP;
			}
			const prefetchedBlobPFromStorage = this.internalStorageService.readBlob(blobId);
			this.prefetchCache.set(
				blobId,
				prefetchedBlobPFromStorage.catch((error: { canRetry?: boolean }) => {
					if (canRetryOnError(error)) {
						this.prefetchCache.delete(blobId);
					}
					throw error instanceof Error ? error : new Error(String(error));
				}),
			);
			return prefetchedBlobPFromStorage;
		}
		return this.internalStorageService.readBlob(blobId);
	}

	private prefetchTree(tree: ISnapshotTree): void {
		const secondary: string[] = [];
		this.prefetchTreeCore(tree, secondary);

		for (const blob of secondary) {
			// We don't care if the prefetch succeeds
			this.cachedRead(blob).catch(() => undefined);
		}
	}

	private prefetchTreeCore(tree: ISnapshotTree, secondary: string[]): void {
		for (const blobKey of Object.keys(tree.blobs)) {
			const blob = tree.blobs[blobKey];
			if (blobKey.startsWith(".") || blobKey === "header" || blobKey.startsWith("quorum")) {
				if (blob !== undefined) {
					// We don't care if the prefetch succeeds
					this.cachedRead(blob).catch(() => undefined);
				}
			} else if (!blobKey.startsWith("deltas") && blob !== undefined) {
				secondary.push(blob);
			}
		}

		for (const subTree of Object.keys(tree.trees)) {
			this.prefetchTreeCore(tree.trees[subTree], secondary);
		}
	}
}
