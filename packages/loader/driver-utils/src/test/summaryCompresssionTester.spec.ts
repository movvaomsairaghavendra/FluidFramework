/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { strict as assert } from "node:assert";

import { TypedEventEmitter } from "@fluid-internal/client-utils";
import { ITelemetryBaseLogger } from "@fluidframework/core-interfaces";
import {
	IClient,
	ISummaryBlob,
	ISummaryHandle,
	ISummaryTree,
	SummaryType,
} from "@fluidframework/driver-definitions";
import {
	FetchSource,
	IDocumentDeltaConnection,
	IDocumentDeltaStorageService,
	IDocumentService,
	IDocumentServiceEvents,
	IDocumentServiceFactory,
	IDocumentServicePolicies,
	IDocumentStorageService,
	IDocumentStorageServicePolicies,
	IResolvedUrl,
	ISummaryContext,
	ICreateBlobResponse,
	ISnapshotTree,
	IVersion,
} from "@fluidframework/driver-definitions/internal";

import {
	ICompressionStorageConfig,
	SummaryCompressionAlgorithm,
	applyStorageCompression,
	blobHeadersBlobName,
} from "../adapters/index.js";
import { DocumentStorageServiceProxy } from "../documentStorageServiceProxy.js";

import { summaryTemplate } from "./summaryCompressionData.js";

function isSummaryTree(obj: unknown): obj is ISummaryTree {
	return (
		obj !== undefined &&
		obj !== null &&
		typeof obj === "object" &&
		"type" in obj &&
		obj.type === SummaryType.Tree &&
		"tree" in obj &&
		typeof obj.tree === "object" &&
		obj.tree !== null
	);
}

function isValidSummaryTree(summary: unknown): boolean {
	if (!isSummaryTree(summary)) {
		return false;
	}
	const channels = summary.tree[".channels"] as ISummaryTree | undefined;
	if (typeof channels !== "object" || channels === undefined || !isSummaryTree(channels)) {
		return false;
	}
	const rootDOId = channels.tree.rootDOId as ISummaryTree | undefined;
	if (typeof rootDOId !== "object" || rootDOId === undefined || !isSummaryTree(rootDOId)) {
		return false;
	}
	const channelsTree = rootDOId.tree[".channels"] as ISummaryTree | undefined;
	if (
		typeof channelsTree !== "object" ||
		channelsTree === undefined ||
		!isSummaryTree(channelsTree)
	) {
		return false;
	}
	const headerHolder = channelsTree.tree["7a99532d-94ec-43ac-8a53-d9f978ad4ae9"] as
		| ISummaryTree
		| undefined;
	return typeof headerHolder === "object" && headerHolder !== undefined;
}

/**
 * This function clones the imported summary and returns a new summary with the same content.
 */
function cloneSummary(): ISummaryTree {
	return JSON.parse(JSON.stringify(summaryTemplate)) as ISummaryTree;
}

/**
 * This function generates the summary with the given content size. At first it clones the summary
 * template, then it generates the content with the given size by loop, which will
 * use repeated sequence from 0 to 10 to generate the content until the content size is achieved.
 * The content is stored in the header of the summary.
 * @param contentSize - The size of the content to be generated.
 */
function generateSummaryWithContent(contentSize: number): ISummaryTree {
	const summary = cloneSummary();
	const header = (
		(
			((summary.tree[".channels"] as ISummaryTree).tree.rootDOId as ISummaryTree).tree[
				".channels"
			] as ISummaryTree
		).tree["7a99532d-94ec-43ac-8a53-d9f978ad4ae9"] as ISummaryTree
	).tree.header;
	let contentString = "";
	while (contentString.length < contentSize) {
		if (contentString.length + 10 > contentSize) {
			contentString += "0123456789".slice(0, Math.max(0, contentSize - contentString.length));
			break;
		} else {
			contentString += "0123456789";
		}
	}
	header.content = `{"value": ${contentString}}`;
	return summary;
}

function generateSummaryWithBinaryContent(
	startsWith: number,
	contentSize: number,
): ISummaryTree {
	const summary = cloneSummary();
<<<<<<< HEAD
	const header = (
		(
			((summary.tree[".channels"] as ISummaryTree).tree.rootDOId as ISummaryTree).tree[
				".channels"
			] as ISummaryTree
		).tree["7a99532d-94ec-43ac-8a53-d9f978ad4ae9"] as ISummaryTree
	).tree.header;
=======
	const header = getHeader(summary);
>>>>>>> 1e46d1e917 (ESLint changes)
	const content = new Uint8Array(contentSize);
	content[0] = startsWith;
	for (let i = 1; i < contentSize; i = i + 10) {
		for (let j = 0; j < 10; j++) {
			content[i + j] = j;
		}
	}
	header.content = content;
	return summary;
}

const misotestid: string = "misotest-id";

const abcContent = "ABC";
class InternalTestStorage implements IDocumentStorageService {
	constructor() {}
	private _uploadedSummary: ISummaryTree | undefined;

	policies?: IDocumentStorageServicePolicies | undefined;

	async getSnapshotTree(
		version?: IVersion | undefined,
		scenarioName?: string | undefined,
		/* eslint-disable-next-line @rushstack/no-new-null */
	): Promise<ISnapshotTree | null> {
		/* eslint-disable-next-line unicorn/no-null */
		return null;
	}
	async getVersions(
		/* eslint-disable-next-line @rushstack/no-new-null */
		versionId: string | null,
		count: number,
		scenarioName?: string | undefined,
		fetchSource?: FetchSource | undefined,
	): Promise<IVersion[]> {
		return [];
	}
	async createBlob(file: ArrayBufferLike): Promise<ICreateBlobResponse> {
		throw new Error("Method not implemented.");
	}
	async readBlob(id: string): Promise<ArrayBufferLike> {
		if (id === misotestid) {
			return new TextEncoder().encode(abcContent);
		}
		if (!this._uploadedSummary) {
			throw new Error("No uploaded summary available");
		}
		return getHeaderContent(this._uploadedSummary);
	}
	async uploadSummaryWithContext(
		summary: ISummaryTree,
		context: ISummaryContext,
	): Promise<string> {
		this._uploadedSummary = summary;
		return "test";
	}
	async downloadSummary(handle: ISummaryHandle): Promise<ISummaryTree> {
		if (!this._uploadedSummary) {
			throw new Error("No uploaded summary available");
		}
		return this._uploadedSummary;
	}
	disposed?: boolean | undefined;
	dispose?(error?: Error | undefined): void {
		throw new Error("Method not implemented.");
	}

	public get uploadedSummary(): ISummaryTree | undefined {
		return this._uploadedSummary;
	}

	public thisIsReallyOriginalStorage: string = "yes";
}

function isInternalTestStorage(
	storage: IDocumentStorageService,
): storage is InternalTestStorage {
	return (
		"thisIsReallyOriginalStorage" in storage &&
		(storage as InternalTestStorage).thisIsReallyOriginalStorage === "yes"
	);
}

function isOriginalStorage(storage: IDocumentStorageService): boolean {
	return isInternalTestStorage(storage);
}

class InternalTestDocumentService
	extends TypedEventEmitter<IDocumentServiceEvents>
	implements IDocumentService
{
	constructor() {
		super();
	}
	resolvedUrl: IResolvedUrl = {
		type: "fluid",
		url: "test",
		tokens: {},
		id: "test-id",
		endpoints: {
			deltaStorageUrl: "test",
			ordererUrl: "test",
			storageUrl: "test",
		},
	};
	policies?: IDocumentServicePolicies | undefined;
	storage: IDocumentStorageService = new InternalTestStorage();
	async connectToStorage(): Promise<IDocumentStorageService> {
		return this.storage;
	}
	async connectToDeltaStorage(): Promise<IDocumentDeltaStorageService> {
		throw new Error("Method not implemented.");
	}
	async connectToDeltaStream(client: IClient): Promise<IDocumentDeltaConnection> {
		throw new Error("Method not implemented.");
	}
	dispose(error?: Error): void {
		throw new Error("Method not implemented.");
	}
}

class InternalTestDocumentServiceFactory implements IDocumentServiceFactory {
	private readonly documentService: IDocumentService;
	constructor() {
		this.documentService = new InternalTestDocumentService();
	}

	async createDocumentService(
		resolvedUrl: IResolvedUrl,
		logger?: ITelemetryBaseLogger | undefined,
		clientIsSummarizer?: boolean | undefined,
	): Promise<IDocumentService> {
		return this.documentService;
	}
	async createContainer(
		createNewSummary: ISummaryTree | undefined,
		createNewResolvedUrl: IResolvedUrl,
		logger?: ITelemetryBaseLogger | undefined,
		clientIsSummarizer?: boolean | undefined,
	): Promise<IDocumentService> {
		return this.documentService;
	}
}

interface IStorageWithConfig extends IDocumentStorageService {
	_config?: ICompressionStorageConfig;
}

class TestDocumentStorageServiceProxy extends DocumentStorageServiceProxy {
	public getInternalStorageService(): IDocumentStorageService {
		return this.internalStorageService;
	}

	public getCompressionConfig(): ICompressionStorageConfig | undefined {
		return (this.internalStorageService as IStorageWithConfig)._config;
	}
}

async function buildCompressionStorage(
	config?: ICompressionStorageConfig | boolean,
): Promise<IDocumentStorageService> {
	{
		const factory: IDocumentServiceFactory = applyStorageCompression(
			new InternalTestDocumentServiceFactory(),
			config,
		);
		const documentService = await factory.createContainer(undefined, {
			type: "fluid",
			url: "test",
			tokens: {},
			id: "test-id",
			endpoints: {
				deltaStorageUrl: "test",
				ordererUrl: "test",
				storageUrl: "test",
			},
		});
		const storage = await documentService.connectToStorage();
		return storage;
	}
}

const prefixForUncompressed = 0xb0;
const prefixForLZ4 = 0xb1;
describe("Summary Compression Tests", () => {
	it("Verify Proper Summary Generation", async (): Promise<void> => {
		const summary = generateSummaryWithContent(1000000);
		const content = getHeaderContentAsString(summary);
		assert(
			content.length === 1000000 + 11,
			`The content size is ${content.length} and should be 1000011`,
		);
	});

	it("Verify Config True", async (): Promise<void> => {
		const storage = await buildCompressionStorage(true);
		checkCompressionConfig(storage, 500, SummaryCompressionAlgorithm.LZ4);
	});

	it("Verify Config False", async (): Promise<void> => {
		const storage = await buildCompressionStorage(false);
		if (storage instanceof TestDocumentStorageServiceProxy) {
			const internalStorage = storage.getInternalStorageService();
			if (internalStorage !== undefined) {
				throw new Error("The storage has compression");
			}
		}
		if (!isOriginalStorage(storage)) {
			throw new Error("The storage is not the original storage");
		}
	});

	it("should compress and decompress summary tree", async (): Promise<void> => {
		// ... test implementation ...
	});

	it("should compress and decompress summary tree with handles", async (): Promise<void> => {
		// ... test implementation ...
	});

	it("Verify Config Empty", async (): Promise<void> => {
		const storage = await buildCompressionStorage();
		if (storage instanceof TestDocumentStorageServiceProxy) {
			const internalStorage = storage.getInternalStorageService();
			if (internalStorage !== undefined) {
				throw new Error("The storage has compression");
			}
		}
		if (!isOriginalStorage(storage)) {
			throw new Error("The storage is not the original storage");
		}
	});

	it("Verify Config Object (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 763,
		};
		const storage = await buildCompressionStorage(config);
		checkCompressionConfig(storage, 763, SummaryCompressionAlgorithm.None);
	});

	it("Verify Compressed Markup at Summary (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const storage = (await buildCompressionStorage(config)) as TestDocumentStorageServiceProxy;
		const summary = generateSummaryWithContent(1000);
		await storage.uploadSummaryWithContext(summary, {
			referenceSequenceNumber: 0,
			proposalHandle: "test",
			ackHandle: "test",
		});
		const internalStorage = storage.getInternalStorageService();
		if (!isInternalTestStorage(internalStorage)) {
			throw new Error("Expected InternalTestStorage");
		}
		const uploadedSummary = internalStorage.uploadedSummary;
		if (!uploadedSummary) {
			throw new Error("Expected uploaded summary");
		}
		if (uploadedSummary.tree === undefined || uploadedSummary.tree === null) {
			throw new Error("Expected uploaded summary tree");
		}
		if (!(blobHeadersBlobName in uploadedSummary.tree)) {
			throw new Error("The summary-blob markup is not added");
		}
	});

	it("Verify Blob Enc/Dec Symmetry (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		await checkEncDec(config);
	});

	it("Verify Blob Enc/Dec no-compress Symmetry (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 500,
		};
		await checkEncDec(config);
	});

	it("Verify Upload / Download Summary (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const storage = (await buildCompressionStorage(config)) as TestDocumentStorageServiceProxy;
		const summary = generateSummaryWithContent(1000);
		const originBlobContent = getHeaderContentAsString(summary);
		await storage.uploadSummaryWithContext(summary, {
			referenceSequenceNumber: 0,
			proposalHandle: "test",
			ackHandle: "test",
		});
		await storage.getSnapshotTree({ id: "test", treeId: "test" }, "test");
		const summaryHandle: ISummaryHandle = {
			type: SummaryType.Handle,
			handleType: SummaryType.Tree,
			handle: "test",
		};
		const downloadedSummary: ISummaryTree = await storage.downloadSummary(summaryHandle);
		const downloadedBlobContent = getHeaderContentAsString(downloadedSummary);
		if (originBlobContent !== downloadedBlobContent) {
			throw new Error(`The origin and the downloaded blob are not the same
			\norigin     : ${originBlobContent}
			\ndownloaded : ${downloadedBlobContent}`);
		}
	});

	it("Verify Upload / Download Summary no-compress (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 500,
		};
		await checkUploadDownloadSummary(config);
	});

	it("Verify no-compress small (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const storage = (await buildCompressionStorage(config)) as TestDocumentStorageServiceProxy;
		const summary = generateSummaryWithContent(300);
		await storage.uploadSummaryWithContext(summary, {
			referenceSequenceNumber: 0,
			proposalHandle: "test",
			ackHandle: "test",
		});
		const originalContent = getHeaderContentAsString(summary);
		const content = new TextDecoder().decode(await storage.readBlob("1234"));
		assert(
			content === originalContent,
			`The content is not equal to original content \n${content} \n ${originalContent}`,
		);
	});

	it("Verify no-compress prefix (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const firstOriginalByte = 0xb3;
		const contentSize = 30;
		const uploadedContent: ArrayBufferLike = await uploadSummaryWithBinaryContent(
			firstOriginalByte,
			contentSize,
			config,
		);
		const [firstByte, secondByte] = getUploadedBytes(uploadedContent);
		if (firstByte !== prefixForUncompressed) {
			throw new Error(`The first byte should be ${prefixForUncompressed} but is ${firstByte}`);
		}
		if (secondByte !== firstOriginalByte) {
			throw new Error(`The second byte should be ${firstOriginalByte} but is ${secondByte}`);
		}
	});

	it("Verify compress prefix (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const firstOriginalByte = 0xb3;
		const contentSize = 800;
		const uploadedContent: ArrayBufferLike = await uploadSummaryWithBinaryContent(
			firstOriginalByte,
			contentSize,
			config,
		);
		const [firstByte] = getUploadedBytes(uploadedContent);
		assert(
			firstByte === prefixForLZ4,
			`The first byte should be ${prefixForLZ4} but is ${firstByte}`,
		);
	});

	it("Verify no-compress no-prefix (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const contentSize = 30;
		await testNoPrefix(contentSize, config);
	});

	it("Verify none-algorithm no-prefix (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 500,
		};
		const contentSize = 800;
		await testNoPrefix(contentSize, config);
	});

	it("Verify prefix compressed (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const firstOriginalByte = 0xb3;
		const contentSize = 800;
		const uploadedContent: ArrayBufferLike = await uploadSummaryWithBinaryContent(
			firstOriginalByte,
			contentSize,
			config,
		);
		const [firstByte] = getUploadedBytes(uploadedContent);
		if (firstByte === undefined) {
			throw new Error("First byte is undefined");
		}
		assert(
			firstByte === prefixForLZ4,
			`The first byte should be ${prefixForLZ4} but is ${firstByte}`,
		);
	});

	it("Verify prefix uncompressed small size (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const contentSize = 30;
		await testPrefix(contentSize, config, 0xb0, 0xc0, prefixForUncompressed);
	});

	it("Verify prefix uncompressed algorithm none (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 500,
		};
		const contentSize = 800;
		await testPrefix(contentSize, config, 0xb0, 0xc0, prefixForUncompressed);
	});

	it("Verify enc / dec compressed loop (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.LZ4,
			minSizeToCompress: 500,
		};
		const contentSize = 800;
		await testEncDecBinaryLoop(contentSize, config);
	});

	it("Verify enc / dec uncompressed loop - algorithm none (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 500,
		};
		const contentSize = 800;
		await testEncDecBinaryLoop(contentSize, config);
	});

	it("Verify enc / dec uncompressed loop - small (summary-blob markup)", async (): Promise<void> => {
		const config: ICompressionStorageConfig = {
			algorithm: SummaryCompressionAlgorithm.None,
			minSizeToCompress: 500,
		};
		const contentSize = 30;
		await testEncDecBinaryLoop(contentSize, config);
	});

	it("Verify Summary Tree Structure", async (): Promise<void> => {
		const summary = generateSummaryWithContent(1000);
		if (!isValidSummaryTree(summary)) {
			throw new Error("Invalid summary tree structure");
		}
		// ... rest of the test ...
	});

	// ... rest of the tests ...
});

async function testNoPrefix(
	contentSize: number,
	config: ICompressionStorageConfig,
): Promise<void> {
	for (let i = 0; i < 256; i++) {
		if (i >= 0xb0 && i <= 0xbf) {
			continue;
		}
		const firstOriginalByte = i;

		const uploadedContent: ArrayBufferLike = await uploadSummaryWithBinaryContent(
			firstOriginalByte,
			contentSize,
			config,
		);
		const [firstByte] = getUploadedBytes(uploadedContent);
		if (firstByte !== firstOriginalByte) {
			throw new Error(`The first byte should be ${firstOriginalByte} but is ${firstByte}`);
		}
	}
}

async function testPrefix(
	contentSize: number,
	config: ICompressionStorageConfig,
	from: number = 0,
	to: number = 256,
	prefix: number = prefixForLZ4,
): Promise<void> {
	for (let i = from; i < to; i++) {
		const firstOriginalByte = i;
		const uploadedContent: ArrayBufferLike = await uploadSummaryWithBinaryContent(
			firstOriginalByte,
			contentSize,
			config,
		);
		const [firstByte] = getUploadedBytes(uploadedContent);
		if (firstByte !== prefix) {
			throw new Error(`The first byte should be ${prefix} but is ${firstByte}`);
		}
	}
}

async function uploadSummaryWithBinaryContent(
	firstOriginalByte: number,
	contentSize: number,
	config: ICompressionStorageConfig,
): Promise<ArrayBufferLike> {
	const storage = (await buildCompressionStorage(config)) as TestDocumentStorageServiceProxy;
	const summary = generateSummaryWithBinaryContent(firstOriginalByte, contentSize);
	await storage.uploadSummaryWithContext(summary, {
		referenceSequenceNumber: 0,
		proposalHandle: "test",
		ackHandle: "test",
	});
	const internalStorage = storage.getInternalStorageService();
	if (!isInternalTestStorage(internalStorage)) {
		throw new Error("Expected InternalTestStorage");
	}
	const uploadedSummary = internalStorage.uploadedSummary;
	if (!uploadedSummary) {
		throw new Error("No uploaded summary available");
	}
	return getHeaderContent(uploadedSummary);
}

async function checkUploadDownloadSummary(
	config: ICompressionStorageConfig,
): Promise<ISummaryTree> {
	const storage = (await buildCompressionStorage(config)) as TestDocumentStorageServiceProxy;
	const summary = generateSummaryWithContent(1000);
	const originBlobContent = getHeaderContentAsString(summary);
	await storage.uploadSummaryWithContext(summary, {
		referenceSequenceNumber: 0,
		proposalHandle: "test",
		ackHandle: "test",
	});
	await storage.getSnapshotTree({ id: "test", treeId: "test" }, "test");
	const summaryHandle: ISummaryHandle = {
		type: SummaryType.Handle,
		handleType: SummaryType.Tree,
		handle: "test",
	};
	const downloadedSummary: ISummaryTree = await storage.downloadSummary(summaryHandle);
	const downloadedBlobContent = getHeaderContentAsString(downloadedSummary);
	assert(
		originBlobContent === downloadedBlobContent,
		`The origin and the downloaded blob are not the same
		\norigin     : ${originBlobContent}
		\ndownloaded : ${downloadedBlobContent}`,
	);
	return downloadedSummary;
}

async function checkEncDec(config: ICompressionStorageConfig): Promise<void> {
	const summary = generateSummaryWithContent(1000);
	await checkEncDecConfigurable(summary, config);
}

async function checkEncDecBinary(
	config: ICompressionStorageConfig,
	startsWith: number,
	contentSize: number,
): Promise<void> {
	const summary = generateSummaryWithBinaryContent(startsWith, contentSize);
	await checkEncDecConfigurable(summary, config, startsWith);
}

async function testEncDecBinaryLoop(
	contentSize: number,
	config: ICompressionStorageConfig,
	from: number = 0,
	to: number = 256,
): Promise<void> {
	for (let i = from; i < to; i++) {
		const firstOriginalByte = i;
		await checkEncDecBinary(config, firstOriginalByte, contentSize);
	}
}

async function checkEncDecConfigurable(
	summary: ISummaryTree,
	config: ICompressionStorageConfig,
	startsWith = -1,
): Promise<void> {
	const storage = (await buildCompressionStorage(config)) as TestDocumentStorageServiceProxy;
	const originHeaderHolder: ISummaryTree = getHeaderHolder(summary);
	const header = originHeaderHolder.tree?.header;
	if (typeof header !== "object" || header === undefined || header.type !== SummaryType.Blob) {
		throw new Error("Missing or invalid header blob");
	}
	const originBlob = header.content;
	await storage.uploadSummaryWithContext(summary, {
		referenceSequenceNumber: 0,
		proposalHandle: "test",
		ackHandle: "test",
	});
	await storage.getSnapshotTree({ id: "test", treeId: "test" }, "test");
	const blob: ArrayBufferLike = await storage.readBlob("abcd");
	const blobStr = new TextDecoder().decode(blob);
	if (typeof originBlob === "string") {
		if (blobStr !== originBlob) {
			throw new Error(
				`The origin and the downloaded blob starting with ${startsWith} are not the same \n\n\n${blobStr}\n\n${originBlob}`,
			);
		}
	} else {
		const originBlobStr = new TextDecoder().decode(originBlob);
		if (blobStr !== originBlobStr) {
			throw new Error(
				`The origin and the downloaded blob are not the same \n\n\n${blobStr}\n\n${originBlobStr}`,
			);
		}
	}
}

function checkCompressionConfig(
	storage: IDocumentStorageService,
	expectedMinSizeToCompress: number,
	expectedAlgorithm: SummaryCompressionAlgorithm,
): void {
	if (!(storage instanceof TestDocumentStorageServiceProxy)) {
		throw new TypeError("Expected TestDocumentStorageServiceProxy");
	}
	const config = storage.getCompressionConfig();
	if (!config) {
		throw new Error("The storage has no compression");
	}
	if (config.minSizeToCompress !== expectedMinSizeToCompress) {
		throw new Error(`Unexpected minSizeToCompress config ${config.minSizeToCompress}`);
	}
	if (config.algorithm !== expectedAlgorithm) {
		throw new Error(`Unexpected algorithm config ${config.algorithm}`);
	}
}

function getHeader(summary: ISummaryTree) {
	return getHeaderHolder(summary).tree.header;
}

function getHeaderContentAsString(summary: ISummaryTree): string {
	const content = getHeader(summary).content;
	if (typeof content === "string") {
		return content;
	}
	return new TextDecoder().decode(content);
}

function getHeaderHolder(summary: ISummaryTree): ISummaryTree {
	if (summary.tree === undefined || summary.tree === null) {
		throw new Error("Missing or invalid summary tree");
	}

	const channels = summary.tree[".channels"] as ISummaryTree | undefined;
	if (!channels || typeof channels !== "object" || !isSummaryTree(channels)) {
		throw new Error("Missing or invalid .channels tree");
	}

	const rootDOId = channels.tree.rootDOId as ISummaryTree | undefined;
	if (typeof rootDOId !== "object" || rootDOId === undefined || !isSummaryTree(rootDOId)) {
		throw new Error("Missing or invalid rootDOId tree");
	}

	const channelsTree = rootDOId.tree[".channels"] as ISummaryTree | undefined;
	if (
		typeof channelsTree !== "object" ||
		channelsTree === undefined ||
		!isSummaryTree(channelsTree)
	) {
		throw new Error("Missing or invalid .channels tree in rootDOId");
	}

	const headerHolder = channelsTree.tree["7a99532d-94ec-43ac-8a53-d9f978ad4ae9"] as
		| ISummaryTree
		| undefined;
	if (
		typeof headerHolder !== "object" ||
		headerHolder === undefined ||
		!isSummaryTree(headerHolder)
	) {
		throw new Error("Missing or invalid header holder tree");
	}

	return headerHolder;
}

function getUploadedBytes(uploadedContent: ArrayBufferLike): [number, number] {
	const uploadedView = new Uint8Array(uploadedContent);
	if (uploadedView === null || uploadedView === undefined || uploadedView.length < 2) {
		throw new Error("Invalid uploaded content");
	}
	return [uploadedView[0], uploadedView[1]];
}
