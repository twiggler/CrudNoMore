import pgPromise from "pg-promise";
import monitor from "pg-monitor";
import { InferMutation, makeDocument, mutateP, readP } from "./document";
import * as schema from "./example_schema";
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

const connectionString = "postgresql://milicense:milicense@localhost:5432/sync";

const main = async () => {
	const initOptions = {};
	const pgp = pgPromise(initOptions);
	monitor.attach(initOptions);
	const db = await pgp(connectionString);

	const mutation: InferMutation<typeof document> = {
		"public.u": {
			create: [
				{
					id: BigInt(10),
					t1: BigInt(20),
					t2: BigInt(30),
					data: "Data",
				},
			],
			delete: []
		},
		"public.t1": {
			create: [
				{
					id: BigInt(20),
					document: BigInt(1),
				},
			],
			update: []
		},
		"public.t2": {
			create: [
				{
					id: BigInt(30),
					t1: BigInt(20),
				},
			],
		},
	};
	const result = await mutateP(document, 1n, mutation, db);
	assert.strictEqual(result.right.length, 3);

	const faultyMutation: InferMutation<typeof document> = {
		"public.u": {
			create: [
				{
					id: BigInt(11),
					t1: BigInt(20),
					t2: BigInt(30),
					data: "Data2",
				},
			],
		},
	};
	const faultyResult = await mutateP(document, 2n, faultyMutation, db);
	assert.strictEqual(faultyResult.left.length, 1);

	const state = await readP(document, 1n, db);
	assert.deepStrictEqual(state, {
		"public.t1": [
			{
				document: 1,
				id: 20,
			},
		],
		"public.t2": [
			{
				t1: 20,
				id: 30,
			},
		],
		"public.u": [
			{
				id: 10,
				t1: 20,
				t2: 30,
				data: "Data",
			},
		],
	});
};

main();
