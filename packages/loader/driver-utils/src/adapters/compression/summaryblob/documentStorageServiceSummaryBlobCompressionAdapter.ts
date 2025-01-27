/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { IsoBuffer } from "@fluid-internal/client-utils";
import { assert } from "@fluidframework/core-utils/internal";
import {
	ISummaryBlob,
	ISummaryHandle,
	ISummaryTree,
	SummaryObject,
	SummaryType,
} from "@fluidframework/driver-definitions";
import {
	IDocumentStorageService,
	ISummaryContext,
	ISnapshotTree,
	IVersion,
} from "@fluidframework/driver-definitions/internal";
import { compress as lz4Compress, decompress as lz4Decompress } from "lz4js";

import { DocumentStorageServiceProxy } from "../../../documentStorageServiceProxy.js";
import { ICompressionStorageConfig, SummaryCompressionAlgorithm } from "../index.js";

// Constants for compression
const algorithmByte = 0xf0; // 11110000 in binary
/**
 * Name of the blob that contains headers information.
 * @internal
 */
export const blobHeadersBlobName = ".blobHeaders";
const metadataBlobName = ".metadata";

enum CompressionAlgorithm {
	None = 0,
	GZip = 1,
}

// Type declarations for the lz4js functions
const compress = (data: ArrayBufferLike): ArrayBufferLike => {
	const input = IsoBuffer.from(data).buffer;
	const result = (lz4Compress as (input: ArrayBufferLike) => ArrayBufferLike)(input);
	if (!(result instanceof ArrayBuffer)) {
		throw new TypeError("lz4Compress did not return an ArrayBuffer value");
	}
	return result;
};
const decompress = (data: ArrayBufferLike): ArrayBufferLike => {
	const input = IsoBuffer.from(data).buffer;
	const result = (lz4Decompress as (input: ArrayBufferLike) => ArrayBufferLike)(input);
	if (!(result instanceof ArrayBuffer)) {
		throw new TypeError("lz4Decompress did not return an ArrayBuffer value");
	}
	return result;
};

/**
 * This class is a proxy for the IDocumentStorageService that compresses and decompresses blobs in the summary.
 * The identification of the compressed blobs is done by adding a compression markup blob to the summary.
 * Even if the markup blob is present, it does not mean that all blobs are compressed. The blob,
 * which is compressed also contain the compression algorithm enumerated value from the
 * SummaryCompressionAlgorithm enumeration in the first byte . If the blob is not
 * commpressed, it contains the first byte equals to SummaryCompressionAlgorithm.None .
 * In case, the markup blob is present, it is expected that the first byte of the markup blob
 * will contain the info about the compression. If the first byte is not present, it is assumed
 * that the compression is not enabled and no first prefix byte is present in the blobs.
 * @public
 */
export class DocumentStorageServiceCompressionAdapter extends DocumentStorageServiceProxy {
	private _isCompressionEnabled: boolean = false;

	constructor(
		service: IDocumentStorageService,
		private readonly _config: ICompressionStorageConfig,
	) {
		super(service);
	}

	public get service(): IDocumentStorageService {
		return this.internalStorageService;
	}

	/**
	 * This method returns `true` if there is a compression markup byte in the blob, otherwise `false`.
	 * @param blob - The blob to compress.
	 * @returns `true` if there is a compression markup byte in the blob, otherwise `false`.
	 */
	private static hasPrefix(blob: ArrayBufferLike): boolean {
		if (blob.byteLength === 0) {
			return false;
		}
		const bytes = new Uint8Array(blob);
		if (bytes.length === 0) {
			return false;
		}
		const firstByte = bytes.length > 0 ? bytes[0] : undefined;
		if (firstByte === undefined) {
			return false;
		}
		return firstByte === algorithmByte;
	}

	/**
	 * This method reads the first byte from the given blob and maps that byte to the compression algorithm.
	 * @param blob - The maybe compressed blob.
	 * @returns The compression algorithm number
	 */
	private static readAlgorithmFromBlob(blob: ArrayBufferLike): CompressionAlgorithm {
		if (blob.byteLength === 0) {
			throw new TypeError("Empty blob");
		}
		const bytes = new Uint8Array(blob);
		if (bytes.length === 0) {
			throw new TypeError("Empty blob");
		}
		const firstByte = bytes.length > 0 ? bytes[0] : undefined;
		if (firstByte === undefined) {
			throw new TypeError("Invalid blob: no first byte");
		}
		if (firstByte !== algorithmByte) {
			throw new TypeError("Invalid algorithm byte");
		}
		return CompressionAlgorithm.GZip;
	}

	/**
	 * This method writes the given algorithm to the blob as the first byte.
	 * @param blob - The blob to write the algorithm to.
	 * @param algorithm - The algorithm to write.
	 * @returns The blob with the algorithm as the first byte.
	 */
	private static writeAlgorithmToBlob(
		blob: ArrayBufferLike,
		algorithm: number,
	): ArrayBufferLike {
		if (algorithm === SummaryCompressionAlgorithm.None) {
			const bytes = new Uint8Array(blob);
			const firstByte = bytes.length > 0 ? bytes[0] : undefined;
			if (firstByte === undefined) {
				throw new TypeError("Invalid blob: empty content");
			}
			// The following bitwise operation is intentional for byte manipulation
			// eslint-disable-next-line no-bitwise
			if ((firstByte & 0b11110000) !== 0b11110000) {
				return blob;
			}
		}
		assert(algorithm < 0x10, 0x6f5 /* Algorithm should be less than 0x10 */);
		const blobView = new Uint8Array(blob);
		const blobLength = blobView.length;
		const newBlob = new Uint8Array(blobLength + 1);
		// The following bitwise operation is intentional for byte manipulation
		// eslint-disable-next-line no-bitwise
		newBlob[0] = 0xb0 | algorithm;
		newBlob.set(blobView, 1);
		return newBlob.buffer;
	}

	/**
	 * This method removes the algorithm markup prefix from the blob (1 byte)
	 * @param blob - The blob to remove the prefix from.
	 * @returns The blob without the prefix.
	 */
	private static removePrefixFromBlobIfPresent(blob: ArrayBufferLike): ArrayBufferLike {
		const blobView = new Uint8Array(blob);
		return this.hasPrefix(blob) ? IsoBuffer.from(blobView.subarray(1)) : blob;
	}

	/**
	 * This method converts the given argument to Uint8Array. If the parameter is already Uint8Array,
	 * it is just returned as is. If the parameter is string, it is converted to Uint8Array using
	 * TextEncoder.
	 * @param input - The input to convert to Uint8Array.
	 * @returns The Uint8Array representation of the input.
	 */
	private static toBinaryArray(input: string | Uint8Array): Uint8Array {
		return typeof input === "string" ? new TextEncoder().encode(input) : input;
	}

	/**
	 * This method encodes the blob inside the given summary object of the SummaryType.Blob type using the given config
	 * containing  the compression algorithm.
	 * @param input - The summary object to encode.
	 * @param config - The config containing the compression algorithm.
	 * @returns The summary object with the encoded blob.
	 */
	private static readonly blobEncoder = (
		input: SummaryObject,
		config: ICompressionStorageConfig,
	): SummaryObject => {
		if (input.type === SummaryType.Blob) {
			const summaryBlob: ISummaryBlob = input;
			const original: ArrayBufferLike = DocumentStorageServiceCompressionAdapter.toBinaryArray(
				summaryBlob.content,
			);
			const processed: ArrayBufferLike = DocumentStorageServiceCompressionAdapter.encodeBlob(
				original,
				config,
			);
			const newSummaryBlob = {
				type: SummaryType.Blob,
				content: IsoBuffer.from(processed),
			};
			return newSummaryBlob;
		} else {
			return input;
		}
	};

	/**
	 * This method decodes the blob inside the given summary object of the SummaryType.Blob type.
	 * @param input - The summary object to decode.
	 * @returns The summary object with the decoded blob.
	 */
	private static readonly blobDecoder = (input: SummaryObject): SummaryObject => {
		if (input.type === SummaryType.Blob) {
			const summaryBlob: ISummaryBlob = input;
			const original: Uint8Array = DocumentStorageServiceCompressionAdapter.toBinaryArray(
				summaryBlob.content,
			);
			const processed: ArrayBufferLike =
				DocumentStorageServiceCompressionAdapter.decodeBlob(original);
			const newSummaryBlob = {
				type: SummaryType.Blob,
				content: IsoBuffer.from(processed),
			};
			return newSummaryBlob;
		} else {
			return input;
		}
	};

	/**
	 * This method encodes the given blob according to the given config.
	 * @param file - The blob to encode.
	 * @param config - The config to use for encoding.
	 * @returns The encoded blob.
	 */
	private static encodeBlob(
		file: ArrayBufferLike,
		config: ICompressionStorageConfig,
	): ArrayBufferLike {
		let maybeCompressed: ArrayBufferLike;
		let algorithm = config.algorithm;

		if (new Uint8Array(file).length < config.minSizeToCompress) {
			maybeCompressed = file;
			algorithm = SummaryCompressionAlgorithm.None;
		} else {
			switch (algorithm) {
				case SummaryCompressionAlgorithm.None: {
					maybeCompressed = file;
					break;
				}
				case SummaryCompressionAlgorithm.LZ4: {
					maybeCompressed = compress(file);
					break;
				}
				default: {
					throw new Error(`Unknown Algorithm ${config.algorithm}`);
				}
			}
		}

		maybeCompressed = this.writeAlgorithmToBlob(maybeCompressed, algorithm);
		return maybeCompressed;
	}

	/**
	 * This method decodes the given blob.
	 * @param file - The blob to decode.
	 * @returns The decoded blob.
	 */
	private static decodeBlob(file: ArrayBufferLike): ArrayBufferLike {
		let decompressed: ArrayBufferLike;
		let originalBlob: ArrayBufferLike;
		let algorithm: number;

		if (this.hasPrefix(file)) {
			algorithm = this.readAlgorithmFromBlob(file);
			originalBlob = this.removePrefixFromBlobIfPresent(file);
		} else {
			algorithm = SummaryCompressionAlgorithm.None;
			originalBlob = file;
		}

		switch (algorithm) {
			case SummaryCompressionAlgorithm.None: {
				decompressed = originalBlob;
				break;
			}
			case SummaryCompressionAlgorithm.LZ4: {
				decompressed = decompress(originalBlob);
				break;
			}
			default: {
				throw new Error(`Unknown Algorithm ${algorithm}`);
			}
		}

		return decompressed;
	}

	/**
	 * This method traverses the SummaryObject recursively. If it finds the ISummaryBlob object,
	 * it applies encoding/decoding on it according to the given isEncode flag.
	 * @param isEncode - True if the encoding should be applied, false if the decoding should be applied.
	 * @param input - The summary object to traverse.
	 * @param encoder - The encoder function to use.
	 * @param decoder - The decoder function to use.
	 * @param config - The config to use for encoding.
	 * @param context - The summary context.
	 * @returns The summary object with the encoded/decoded blob.
	 */
	private static recursivelyReplace(
		isEncode: boolean,
		input: SummaryObject,
		encoder: (input: SummaryObject, config: ICompressionStorageConfig) => SummaryObject,
		decoder: (input: SummaryObject) => SummaryObject,
		config: ICompressionStorageConfig,
		context?: ISummaryContext,
	): SummaryObject {
		switch (input.type) {
			case SummaryType.Tree: {
				const summaryTree: ISummaryTree = input;
				const result: ISummaryTree = {
					...summaryTree,
					tree: { ...summaryTree.tree },
				};
				for (const [key, value] of Object.entries(summaryTree.tree)) {
					if (value !== undefined && typeof value === "object" && "type" in value) {
						const processedValue = this.recursivelyReplace(
							isEncode,
							value,
							encoder,
							decoder,
							config,
							context,
						);
						result.tree[key] = processedValue;
					}
				}
				return result;
			}
			case SummaryType.Blob: {
				return isEncode ? encoder(input, config) : decoder(input);
			}
			case SummaryType.Handle: {
				return input;
			}
			default: {
				throw new TypeError(`Unknown summary type: ${JSON.stringify(input)}`);
			}
		}
	}

	/**
	 * Puts the compression markup in the given summary tree.
	 * @param summary - The summary tree to put the compression markup in.
	 */
	private static putCompressionMarkup(
		summary: ISummaryTree,
		config: ICompressionStorageConfig,
	): void {
		if (summary.tree === undefined) {
			summary.tree = {};
		}

		const blobHeaders: ISummaryBlob = {
			type: SummaryType.Blob,
			content: JSON.stringify({}),
		};
		summary.tree[blobHeadersBlobName] = blobHeaders;
	}

	/**
	 * This method traverses the SnapshotTree recursively. If it finds the ISummaryBlob object with the key '.metadata',
	 * it checks, if the SummaryTree holder of that object also contains the compression markup blob. If it is found,
	 * it returns true, otherwise false.
	 * @param snapshot - The snapshot tree to traverse.
	 * @returns True if the compression markup blob is found, otherwise false.
	 */
	private static hasCompressionMarkup(snapshot: ISnapshotTree): boolean {
		if (snapshot.blobs === undefined || snapshot.trees === undefined) {
			return false;
		}

		// Check if the metadata blob and compression markup exist
		const hasMetadataBlob = snapshot.blobs[metadataBlobName] !== undefined;
		const hasCompressionMarkupBlob = snapshot.blobs[blobHeadersBlobName] !== undefined;
		if (hasMetadataBlob && hasCompressionMarkupBlob) {
			return true;
		}

		// Recursively check all subtrees
		for (const tree of Object.values(snapshot.trees)) {
			if (tree !== undefined && this.hasCompressionMarkup(tree)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * This method performs compression of the blobs in the summary tree.
	 * @param summary - The summary tree to compress.
	 * @param config - The compression config.
	 * @returns The compressed summary tree.
	 */
	public static compressSummary(
		summary: ISummaryTree,
		config: ICompressionStorageConfig,
	): ISummaryTree {
		const result = DocumentStorageServiceCompressionAdapter.recursivelyReplace(
			true,
			summary,
			DocumentStorageServiceCompressionAdapter.blobEncoder,
			DocumentStorageServiceCompressionAdapter.blobDecoder,
			config,
		) as ISummaryTree;

		DocumentStorageServiceCompressionAdapter.putCompressionMarkup(result, config);
		return result;
	}

	/**
	 * This method read blob from the storage and decompresses it if it is compressed.
	 * @param id - The id of the blob to read.
	 * @returns The decompressed blob.
	 */
	public override async readBlob(id: string): Promise<ArrayBufferLike> {
		const originalBlob = await super.readBlob(id);
		if (this._isCompressionEnabled) {
			const decompressedBlob =
				DocumentStorageServiceCompressionAdapter.decodeBlob(originalBlob);
			//			console.log(`Miso summary-blob Blob read END : ${id} ${decompressedBlob.byteLength}`);
			return decompressedBlob;
		} else {
			return originalBlob;
		}
	}

	/**
	 * This method loads the snapshot tree from the server. It also checks, if the compression markup blob is present
	 * and setups the compression flag accordingly. It also identifies the blobs that are not compressed and do not contain
	 * algorithm byte prefix and store them.
	 * @param version - The version of the snapshot tree to load.
	 * @param scenarioName - The scenario name of the snapshot tree to load.
	 * @returns The snapshot tree.
	 */
	public override async getSnapshotTree(
		version?: IVersion,
		scenarioName?: string,
		// eslint-disable-next-line @rushstack/no-new-null
	): Promise<ISnapshotTree | null> {
		const snapshotTree = await super.getSnapshotTree(version, scenarioName);
		const hasMarkup =
			snapshotTree !== undefined &&
			snapshotTree !== null &&
			DocumentStorageServiceCompressionAdapter.hasCompressionMarkup(snapshotTree);
		this._isCompressionEnabled = hasMarkup;
		return snapshotTree;
	}

	/**
	 * This method uploads the summary to the storage. It performs compression of the blobs in the summary tree.
	 * @param summary - The summary tree to upload.
	 * @param context - The summary context.
	 * @returns The ID of the uploaded summary.
	 */
	public override async uploadSummaryWithContext(
		summary: ISummaryTree,
		context: ISummaryContext,
	): Promise<string> {
		const prep = DocumentStorageServiceCompressionAdapter.compressSummary(
			summary,
			this._config,
		);
		return super.uploadSummaryWithContext(prep, context);
	}

	/**
	 * This method downloads the summary from the storage and then applies decompression on the compressed blobs.
	 * @param id - The ID of the summary to be downloaded
	 * @returns The summary with decompressed blobs
	 */
	public override async downloadSummary(id: ISummaryHandle): Promise<ISummaryTree> {
		const summary = await super.downloadSummary(id);
		if (!this._isCompressionEnabled) {
			return summary;
		}
		const decompressedSummary = DocumentStorageServiceCompressionAdapter.recursivelyReplace(
			false,
			summary,
			DocumentStorageServiceCompressionAdapter.blobEncoder,
			DocumentStorageServiceCompressionAdapter.blobDecoder,
			this._config,
		);
		return decompressedSummary as ISummaryTree;
	}
}
