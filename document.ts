import { Column, Reference, qualifiedTableName, toTable, fromTable } from "./schema";
import { apply, pipe } from "fp-ts/lib/function";
import * as RA from "fp-ts/lib/ReadonlyArray";
import * as NRA from "fp-ts/lib/ReadonlyNonEmptyArray";
import * as S from "fp-ts/lib/string";
import * as RR from "fp-ts/lib/ReadonlyRecord";
import * as O from "fp-ts/lib/Option";
import * as E from "fp-ts/lib/Either";
import * as M from "fp-ts/lib/Map";
import * as MD from "fp-ts/lib/Monoid";
import * as RTE from "fp-ts/lib/ReaderTaskEither";
import { Edge, Graph, makeGraph, precedingEdges, topsort } from "./graph";
import { Path, Rows, secureInsert, secureRead, TraceResult } from "./postgres";
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
): RTE.ReaderTaskEither<pg.IDatabase<Ext>, Error, TraceResult> => {
	const [graph, root] = toGraph(document);
	const tableToPrecedingEdge = precedingEdges(root, graph);

	return pipe(
		topsort(root, graph),
		RA.filterMap((tableName) =>
			sequenceT(O.Applicative)(
				O.some(tableName),
				pipe(
					RR.lookup<TableMutation | undefined>(tableName, mutation),
					O.chain((mutation) => O.fromNullable(mutation?.create)),
					O.chain(NRA.fromReadonlyArray)
				)
			)
		),
		RTE.traverseSeqArray(([tableName, values]) =>
			pipe(
				E.right(
					(columns: NRA.ReadonlyNonEmptyArray<Column>) =>
						(paths: NRA.ReadonlyNonEmptyArray<Path>) =>
							secureInsert(id, values, columns, paths)
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
		RTE.map(MD.concatAll(monoidTraceResult))
	);
};

export const mutateP = async <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>,
	mutation: InferMutation<D>,
	dbConn: pg.IDatabase<Ext>
): Promise<TraceResult> =>
	mutate(
		document,
		id,
		mutation
	)(dbConn)().then(
		E.fold(
			(e: Error) => Promise.reject(e),
			(v: TraceResult) => Promise.resolve(v)
		)
	);

export const read = <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>
): RTE.ReaderTaskEither<pg.IDatabase<Ext>, Error, InferReadModel<Document>> => {
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
					pg.IDatabase<Ext>,
					Error,
					InferReadModel<Document>
				>
		)
	);
};

export const readP = async <D extends Document, Ext>(
	document: D,
	id: InferDocumentRootType<D>,
	dbConn: pg.IDatabase<Ext>
): Promise<InferReadModel<Document>> =>
	read(
		document,
		id
	)(dbConn)().then(
		E.fold(
			(e: Error) => Promise.reject(e),
			(v: InferReadModel<Document>) => Promise.resolve(v)
		)
	);

export type TableMutations<C extends readonly Column[] = readonly Column[]> = {
	[TableName in InferTables<C>]?: TableMutation<
		FilterColumns<C[number], "qualifiedTableName", TableName>
	>;
};

export interface TableMutation<C extends Column = never> {
	readonly create?: TableRow<C>[];
	readonly update?: TableRow<C>[];
	readonly delete?: [C] extends [never]
		? unknown
		: InferColumnType<FilterColumns<C, "isPrimary", true>>[];
}

export type ReadModels<C extends readonly Column[] = readonly Column[]> = {
	[TableName in InferTables<C>]: ReadModel<
		FilterColumns<C[number], "qualifiedTableName", TableName>
	>;
};

export type ReadModel<C extends Column = never> = {
	[ColumnName in C["name"]]?: InferColumnType<FilterColumns<C, "name", ColumnName>>;
};

export type TableRow<C extends Column = never> = [C] extends [never]
	? { [columnName: string]: unknown }
	: {
			[ColumnName in C["name"]]?: InferColumnType<FilterColumns<C, "name", ColumnName>>;
	  };

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

const filterColumnsByTable = <D extends Document>(document: D, tableName: string) =>
	pipe(
		document,
		columns,
		RA.filter(contramap(qualifiedTableName)((x) => x === tableName)),
		NRA.fromReadonlyArray,
		E.fromOption(() => "No columns found")
	);

const filterReferencesByTable = <D extends Document>(document: D, tableName: string) =>
	pipe(document, references, RA.filter(contramap(fromTable)((x) => x === tableName)));

const monoidTraceResult: MD.Monoid<TraceResult> = MD.struct({
	left: RA.getMonoid<Rows>(),
	right: RA.getMonoid<Rows>(),
});

type InferTables<C extends readonly Column[]> = C[number]["qualifiedTableName"];

type InferColumnType<C> = C extends Column<any, any, infer Type, any> ? Type : never;

type FilterColumns<C extends Column, K extends keyof C, V> = C extends { [X in K]: V } ? C : never;

type InferDocumentRootType<D> = D extends Document<infer Root, readonly Column[]>
	? InferColumnType<Root>
	: never;

type InferDocumentColumns<D> = D extends Document<infer Root, infer Columns>
	? [Root, ...Columns]
	: never;
