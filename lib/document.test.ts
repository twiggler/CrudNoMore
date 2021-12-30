import * as dotenv from "dotenv";
import pgPromise, { QueryFile } from "pg-promise";
import monitor from "pg-monitor";
import path from "path";
import assert from "assert";
import { InferMutation, makeDocument, mutateP, readP } from "./document";
import * as schema from "./test/schema";
import { t1Data, t2Data, uData } from "./test/rows";

const configPath = path.resolve(__dirname, "test", ".env");
dotenv.config({ path: configPath });

const connectionString =
	process.env["TESTDB_CONNECTION_STRING"] ??
	"postgresql://sync-test:sync-test@localhost:5432/sync-test";
const logSql = process.env["LOG_SQL"] ?? false;

describe("document", async () => {
	const initOptions = {};
	const pgp = pgPromise(initOptions);
	if (logSql) monitor.attach(initOptions);
	const db = pgp(connectionString);

	const schemaQuery = new QueryFile(path.resolve(__dirname, "test", "schema.sql"));
	const resetQuery = new QueryFile(path.resolve(__dirname, "test", "reset.sql"));
	const readQuery = new QueryFile(path.resolve(__dirname, "test", "read.sql"));
	const populateQuery = new QueryFile(path.resolve(__dirname, "test", "populate.sql"));

	const document = makeDocument(
		schema.publicDocumentId,
		[
			schema.publicT1Document,
			schema.publicT1Id,
			schema.publicT2Id,
			schema.publicT2T1,
			schema.publicUData,
			schema.publicUId,
			schema.publicUT1,
			schema.publicUT2,
		] as const,
		[
			schema.publicT1IdBypublicT2T1,
			schema.publicT1IdBypublicUT1,
			schema.publicT2IdBypublicUT2,
			schema.publicDocumentIdBypublicT1Document,
		]
	);

	before(async () => {
		await db.none(schemaQuery);
	});

	beforeEach(async () => {
		await db.any(resetQuery);
	});

	describe("create", () => {
		it("should create rows", async () => {
			const mutation: InferMutation<typeof document> = {
				"public.u": {
					create: [uData],
				},
				"public.t1": {
					create: [t1Data],
				},
				"public.t2": {
					create: [t2Data],
				},
			};

			const mutationErrors = await mutateP(document, 1, mutation, db);
			const { document: documentData } = await db.one(readQuery);

			assert.strictEqual(mutationErrors.length, 0);
			assert.deepStrictEqual(documentData, {
				"public.u": [uData],
				"public.t1": [t1Data],
				"public.t2": [t2Data],
			});
		});

		it("should fail when creating rows in a nonexisting document", async () => {
			const mutation: InferMutation<typeof document> = {
				"public.u": {
					create: [uData],
				},
				"public.t1": {
					create: [t1Data],
				},
				"public.t2": {
					create: [t2Data],
				},
			};

			const mutationErrors = await mutateP(document, -999, mutation, db);
			const { document: documentData } = await db.one(readQuery);

			assert.deepStrictEqual(mutationErrors, [
				{ op: "CREATE", table: "public.t1", index: 0, error: "DOCUMENT_NOT_FOUND" },
				{ op: "CREATE", table: "public.t2", index: 0, error: "DOCUMENT_NOT_FOUND" },
				{ op: "CREATE", table: "public.u", index: 0, error: "DOCUMENT_NOT_FOUND" },
			]);
			assert.deepStrictEqual(documentData, {
				"public.u": [],
				"public.t1": [],
				"public.t2": [],
			});
		});

		it("should fail when creating rows with conflicting documents", async () => {
			const t1Data2 = { id: 21, document: 2 };
			const mutation: InferMutation<typeof document> = {
				"public.t1": {
					create: [t1Data, t1Data2],
				},
				"public.t2": {
					create: [t2Data],
				},
				"public.u": {
					create: [
						{
							id: 10,
							t1: t1Data2.id,
							t2: 30,
						},
					],
				},
			};

			const mutationErrors = await mutateP(document, 1, mutation, db);
			const { document: documentData } = await db.one(readQuery);

			assert.deepStrictEqual(mutationErrors, [
				{ op: "CREATE", table: "public.u", index: 0, error: "DOCUMENT_NOT_FOUND" },
			]);
			assert.deepStrictEqual(documentData, {
				"public.u": [],
				"public.t1": [t1Data, t1Data2],
				"public.t2": [t2Data],
			});
		});
	});

	describe("read", () => {
		beforeEach(async () => {
			await db.none(populateQuery);
		});

		it("should read documents", async () => {
			const documentData = await readP(document, 1, db);

			assert.deepStrictEqual(documentData, {
				"public.document": { id: 1 },
				"public.t1": [
					{ id: 10, document: 1 },
					{ id: 11, document: 1 },
				],
				"public.t2": [
					{
						id: 20,
						t1: 10,
					},
				],
				"public.u": [
					{
						id: 30,
						t1: 10,
						t2: 20,
						data: "Data1",
					},
					{
						id: 31,
						t1: 10,
						t2: 20,
						data: "Data2",
					},
				],
			});
		});
	});
});
