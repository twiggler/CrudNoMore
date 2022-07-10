import pgPromise, { QueryFile } from "pg-promise";
import monitor from "pg-monitor";
import path from "path";
import { InferMutation, makeDocument, mutateP, readP } from "../src/lib/document";
import * as schema from "./schema";
import assert from "assert";

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

const connectionString = "postgresql://postgres:postgres@localhost:5433/postgres";

const resetQuery = new QueryFile(path.resolve(__dirname, "reset.sql"));

const main = async () => {
	const initOptions = {};
	const pgp = pgPromise(initOptions);
	monitor.attach(initOptions);
	const db = await pgp(connectionString);

	await db.any(resetQuery);

	const create: InferMutation<typeof document> = {
		"public.u": {
			create: [
				{
					id: 10,
					t1: 20,
					t2: 30,
					data: "Data1",
				},
				{
					id: 11,
					t1: 20,
					t2: 30,
					data: "Data2",
				},
			],
		},
		"public.t1": {
			create: [
				{
					id: 20,
					document: 1,
				},
			],
		},
		"public.t2": {
			create: [
				{
					id: 30,
					t1: 20,
				},
			],
		},
	};
	const creationErrors = await mutateP(document, 1, create, db);
	assert.strictEqual(creationErrors.length, 0);

	const documentData = await readP(document, 1, db);
	assert.deepStrictEqual(documentData, {
		"public.document": {
			id: 1,
		},
		"public.t1": [{ id: 20, document: 1 }],
		"public.t2": [
			{
				id: 30,
				t1: 20,
			},
		],
		"public.u": [
			{
				id: 10,
				t1: 20,
				t2: 30,
				data: "Data1",
			},
			{
				id: 11,
				t1: 20,
				t2: 30,
				data: "Data2",
			},
		],
	});

	const updateDelete: InferMutation<typeof document> = {
		"public.u": {
			update: [
				{
					id: 10,
					data: "Update1",
				},
			],
			delete: [11],
		},
	};
	const updateDeleteErrors = await mutateP(document, 1, updateDelete, db);

	const updatedDocumentData = await readP(document, 1, db);
	assert.deepStrictEqual(updatedDocumentData, {
		"public.document": {
			id: 1,
		},
		"public.t1": [{ id: 20, document: 1 }],
		"public.t2": [
			{
				id: 30,
				t1: 20,
			},
		],
		"public.u": [
			{
				id: 10,
				t1: 20,
				t2: 30,
				data: "Update1",
			},
		],
	});
};

main();
