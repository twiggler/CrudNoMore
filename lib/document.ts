import { Column, Reference, qualifiedTableName, toTable, fromTable, isPrimary } from "./schema";
import { apply, flow, pipe } from "fp-ts/lib/function";
import * as RA from "fp-ts/lib/ReadonlyArray";
import * as NRA from "fp-ts/lib/ReadonlyNonEmptyArray";
import * as S from "fp-ts/lib/string";
import * as O from "fp-ts/lib/Option";
import * as E from "fp-ts/lib/Either";
import * as M from "fp-ts/lib/Map";
import * as RTE from "fp-ts/lib/ReaderTaskEither";
import { Edge, Graph, makeGraph, precedingEdges, topsort } from "./graph";
import { Path, secureInsert, secureRead, secureUpdate } from "./postgres";
import { contramap } from "fp-ts/lib/Predicate";
import { sequenceT } from "fp-ts/lib/Apply";
import * as pg from "pg-promise";

export interface Document<
	R extends Column = Column,
	O extends readonly Column[] = readonly Column[]
> {
	readonly columns: readonly [R, ...O];
	readonly references: readonly Reference[];
}

export type InferMutation<D extends Document> = TableMutations<InferDocumentColumns<D>>;

export type InferReadModel<D extends Document> = ReadModels<InferDocumentColumns<D>>;

export const makeDocument = <R extends Column, C extends readonly Column[]>(
	root: R,
	columns: C,
	references: readonly Reference[]
): Document<R, C> => ({
	columns: [root, ...columns],
	references,
});

export const columns = (document: Document): NRA.ReadonlyNonEmptyArray<Column> => document.columns;

export const references = (document: Document): readonly Reference[] => document.references;

export const mutate = <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>,
	mutation: InferMutation<D>
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly CrudResult[]> => {
	const [graph, root] = toGraph(document);
	const tableToPrecedingEdge = precedingEdges(root, graph);

	return pipe(
		topsort(root, graph) as readonly [InferTables<InferDocumentColumns<D>>],
		RA.filterMap((tableName) =>
			sequenceT(O.Applicative)(O.some(tableName), O.fromNullable(mutation[tableName]))
		),
		RTE.traverseSeqArray(([tableName, mutation]) =>
			pipe(
				E.right(
					(columns: NRA.ReadonlyNonEmptyArray<Column>) =>
						(paths: NRA.ReadonlyNonEmptyArray<Path>) =>
							pipe(
								[
									makeCrudTask(
										secureInsert(id, tableName, columns, paths),
										makeCreateResult(tableName),
										mutation.create
									),
									makeCrudTask(
										secureUpdate(id, tableName, columns, paths),
										makeUpdateResult(tableName),
										mutation.update
									),
								],
								RA.compact,
								RTE.sequenceSeqArray,
								RTE.map(RA.flatten)
							)
				),
				E.ap(filterColumnsByTable(document, tableName)),
				E.ap(
					pipe(
						filterReferencesByTable(document, tableName),
						E.fromOptionK(() => "No references found")(NRA.fromReadonlyArray),
						E.chain((refs) => paths(root, tableToPrecedingEdge, refs))
					)
				),
				E.getOrElse((err) => RTE.left(Error(err)))
			)
		),
		RTE.map(RA.flatten)
	);
};

export const mutateP = async <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>,
	mutation: InferMutation<D>,
	dbProtocol: pg.IBaseProtocol<Ext>
): Promise<readonly CrudResult[]> =>
	mutate(
		document,
		id,
		mutation
	)(dbProtocol)().then(
		E.fold(
			(e) => Promise.reject(e),
			(v) => Promise.resolve(v)
		)
	);

// Assumes foreign keys are non-nullable.
export const read = <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, InferReadModel<Document>> => {
	const [graph, root] = toGraph(document);
	const tableToPrecedingEdge = precedingEdges(root, graph);

	return pipe(
		topsort(root, graph),
		RA.tail,
		O.chain(RA.traverse(O.Applicative)((table) => M.lookup(S.Eq)(table, tableToPrecedingEdge))),
		O.map(RA.map((edge) => edge.label)),
		O.chain(NRA.fromReadonlyArray),
		O.fold(
			() => RTE.left(Error("Tables are not connected.")),
			(refs) =>
				secureRead(id, document.columns, refs) as RTE.ReaderTaskEither<
					pg.IBaseProtocol<Ext>,
					Error,
					InferReadModel<Document>
				>
		)
	);
};

export const readP = async <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>,
	dbProtocol: pg.IBaseProtocol<Ext>
): Promise<InferReadModel<Document>> =>
	read(
		document,
		id
	)(dbProtocol)().then(
		E.fold(
			(e: Error) => Promise.reject(e),
			(v: InferReadModel<Document>) => Promise.resolve(v)
		)
	);

export type TableMutations<C extends readonly Column[]> = {
	[TableName in InferTables<C>]?: TableMutation<
		Partition<FilterColumns<C[number], "qualifiedTableName", TableName>>
	>;
};

export interface TableMutation<P extends [Column, Column]> {
	readonly create?: TableRow<P>[];
	readonly update?: TableRow<P>[];
	readonly delete?: InferColumnType<P[0]>;
}

export type ReadModels<C extends readonly Column[]> = {
	[TableName in InferTables<C>]: ReadModel<
		FilterColumns<C[number], "qualifiedTableName", TableName>
	>;
};

export type ReadModel<C extends Column> = {
	[ColumnName in C["name"]]: InferColumnType<FilterColumns<C, "name", ColumnName>>;
};

export type TableRow<P extends [Column, Column]> = {
	[ColumnName in P[0]["name"]]: InferColumnType<FilterColumns<P[0], "name", ColumnName>>;
} &
	{
		[ColumnName in P[1]["name"]]?: InferColumnType<FilterColumns<P[1], "name", ColumnName>>;
	};

export type MutationError = "DOCUMENT_NOT_FOUND";

export type CrudResult = CreateResult | UpdateResult;

export interface CreateResult {
	table: string;
	index: number;
	error: MutationError;
}

const makeCreateResult =
	(table: string) =>
	(index: number, error: MutationError): CreateResult => ({
		table,
		index,
		error,
	});

export interface UpdateResult {
	table: string;
	primaryKey: unknown;
	error: MutationError;
}

const makeUpdateResult =
	(table: string) =>
	(primaryKey: unknown, error: MutationError): UpdateResult => ({
		table,
		primaryKey,
		error,
	});

const toGraph = (document: Document): readonly [Graph<Reference>, string] =>
	pipe(
		(ts: NRA.ReadonlyNonEmptyArray<string>) => (edges: readonly Edge<Reference>[]) =>
			[makeGraph(ts, edges), NRA.head(ts)] as const,
		apply(pipe(document, columns, tableNames)),
		apply(pipe(document, references, RA.map(toEdge)))
	);

const toEdge = (reference: Reference): Edge<Reference> => ({
	from: qualifiedTableName(reference.to),
	to: qualifiedTableName(reference.from),
	label: reference,
});

const tableNames = (
	columns: NRA.ReadonlyNonEmptyArray<Column>
): NRA.ReadonlyNonEmptyArray<string> => pipe(columns, NRA.map(qualifiedTableName), NRA.uniq(S.Eq));

const paths = (
	rootTable: string,
	precedingEdges: Map<string, Edge<Reference>>,
	references: NRA.ReadonlyNonEmptyArray<Reference>
): E.Either<string, NRA.ReadonlyNonEmptyArray<Path>> =>
	pipe(
		references,
		NRA.traverse(E.Applicative)((ref) =>
			pipe(
				RA.unfold(toTable(ref), (r) =>
					pipe(
						M.lookup(S.Eq)(r, precedingEdges),
						O.map((edge: Edge<Reference>) => [edge.label, toTable(edge.label)] as const)
					)
				),
				RA.prepend(ref),
				E.fromPredicate(
					(path) => toTable(NRA.last(path)) === rootTable,
					() => "No path to document root found"
				)
			)
		)
	);

const filterColumnsByTable = <D extends Document>(
	document: D,
	tableName: InferTables<InferDocumentColumns<D>>
) =>
	pipe(
		document,
		columns,
		RA.filter(contramap(qualifiedTableName)((x) => x === tableName)),
		(columns) =>
			pipe(
				RA.findIndex(isPrimary)(columns),
				O.chain((i) => pipe(columns, RA.prepend(columns[i]), RA.deleteAt(i + 1)))
			),
		O.chain(NRA.fromReadonlyArray),
		E.fromOption(() => "No columns found")
	);

const filterReferencesByTable = <D extends Document>(
	document: D,
	tableName: InferTables<InferDocumentColumns<D>>
) => pipe(document, references, RA.filter(contramap(fromTable)((x) => x === tableName)));

type CrudTask<Ext, P extends [Column, Column], R> = (
	values: NRA.ReadonlyNonEmptyArray<TableRow<P>>
) => RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly R[]>;

const makeCrudTask = <Ext, P extends [Column, Column], R>(
	op: CrudTask<Ext, P, R>,
	parseResult: (index: R, error: MutationError) => CrudResult,
	values?: readonly TableRow<P>[]
): O.Option<RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly CrudResult[]>> =>
	pipe(
		values,
		O.fromNullable,
		O.chain(NRA.fromReadonlyArray),
		O.map((values) => op(values)),
		O.map(RTE.map(RA.map((index) => parseResult(index, "DOCUMENT_NOT_FOUND"))))
	);

type InferTables<C extends readonly Column[]> = C[number]["qualifiedTableName"];

type InferColumnType<C> = C extends Column<any, any, infer Type, any> ? Type : never;

type FilterColumns<C extends Column, K extends keyof C, V> = C extends { [X in K]: V } ? C : never;

type Partition<C extends Column> = [
	FilterColumns<C, "isPrimary", true>,
	FilterColumns<C, "isPrimary", false>
];

type InferDocumentRootType<D> = D extends Document<infer Root, readonly Column[]>
	? InferColumnType<Root>
	: never;

type InferDocumentColumns<D> = D extends Document<infer Root, infer Columns>
	? [Root, ...Columns]
	: never;
