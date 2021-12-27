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
					create: uData,
				},
				"public.t1": {
					create: t1Data,
				},
				"public.t2": {
					create: t2Data,
				},
			};

			const result = await mutateP(document, 1, mutation, db);
			const { document: documentData } = await db.one(readQuery);

			assert.strictEqual(result.right.length, 3);
			assert.strictEqual(result.left.length, 0);
			assert.deepStrictEqual(documentData, {
				"public.u": uData,
				"public.t1": t1Data,
				"public.t2": t2Data,
			});
		});

		it("it should fail when creating rows in a nonexisting document", async () => {
			const mutation: InferMutation<typeof document> = {
				"public.u": {
					create: uData,
				},
				"public.t1": {
					create: t1Data,
				},
				"public.t2": {
					create: t2Data,
				},
			};

			const result = await mutateP(document, 2, mutation, db);
			const { document: documentData } = await db.one(readQuery);

			assert.strictEqual(result.right.length, 0);
			assert.strictEqual(result.left.length, 3);
			assert.deepStrictEqual(documentData, {
				"public.u": [],
				"public.t1": [],
				"public.t2": [],
			});
		});
	});
});
