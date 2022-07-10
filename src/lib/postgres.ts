import { apply, flow, pipe } from "fp-ts/lib/function";
import * as RA from "fp-ts/lib/ReadonlyArray";
import * as NRA from "fp-ts/lib/ReadonlyNonEmptyArray";
import * as RR from "fp-ts/lib/ReadonlyRecord";
import * as S from "fp-ts/String";
import { intercalate } from "fp-ts/lib/Foldable";
import {
	Column,
	columnDbType,
	columnName,
	fromColumn,
	qualifiedColumnName,
	qualifiedTableName,
	Reference,
	toColumn,
	toTable,
} from "./schema";
import * as SG from "fp-ts/lib/Semigroup";
import * as O from "fp-ts/lib/Option";
import * as Eq from "fp-ts/lib/Eq";
import * as pg from "pg-promise";
import * as RT from "fp-ts/lib/ReadonlyTuple";
import * as TE from "fp-ts/lib/TaskEither";
import * as N from "fp-ts/lib/number";
import * as SEP from "fp-ts/lib/Separated";
import * as RTE from "fp-ts/lib/ReaderTaskEither";

type Row = RR.ReadonlyRecord<string, unknown>;

type Values = NRA.ReadonlyNonEmptyArray<Row>;

export type Path = NRA.ReadonlyNonEmptyArray<Reference>;

export const secureInsert = <Ext>(
	documentKey: unknown,
	tableName: string,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	paths: NRA.ReadonlyNonEmptyArray<Path>,
	values: Values
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly number[]> =>
	pipe(
		select("rnum"),
		from("trace"),
		cte(
			"trace",
			pipe(imputeValues(values, columns, "default"), (augmented) =>
				backtraceNewQ(documentKey, augmented, columns, paths)
			)
		),
		cte("ir", insertQ(tableName, columns, "trace")),
		stringifyQuery,
		executeQueryAny,
		RTE.map(RA.map((result: { rnum: number }) => result.rnum)),
		RTE.map((trace) => RA.difference(N.Eq)(NRA.range(0, values.length - 1), trace))
	);

export const secureUpdate = <Ext>(
	documentKey: unknown,
	tableName: string,
	[primary, ...rest]: NRA.ReadonlyNonEmptyArray<Column>,
	paths: NRA.ReadonlyNonEmptyArray<Path>,
	values: Values
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly unknown[]> =>
	pipe(
		select(as(primary, "id")),
		from("ir"),
		cte(
			"current",
			pipe(selectColumn(values, primary), (ids) =>
				backtraceExistingQ(documentKey, ids, primary, paths)
			)
		),
		cte(
			"new",
			pipe(imputeValues(values, [primary, ...rest], null), (imputed) =>
				backtraceUpdatesQ(documentKey, imputed, [primary, ...rest], paths)
			)
		),
		cte("ir", updateQ(tableName, [primary, ...rest], "current", "new")),
		stringifyQuery,
		executeQueryAny,
		RTE.map(RA.map((result: { id: unknown }) => result.id)),
		RTE.map((trace) => RA.difference(Eq.eqStrict)(selectColumn(values, primary), trace))
	);

export const secureDelete = <Ext>(
	documentKey: unknown,
	tableName: string,
	primary: Column,
	paths: NRA.ReadonlyNonEmptyArray<Path>,
	ids: NRA.ReadonlyNonEmptyArray<unknown>
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly unknown[]> =>
	pipe(
		select(as(primary, "id")),
		from("del"),
		cte("trace", backtraceExistingQ(documentKey, ids, primary, paths)),
		cte("del", deleteQ(tableName, primary, "trace")),
		stringifyQuery,
		executeQueryAny,
		RTE.map(RA.map((result: { id: unknown }) => result.id)),
		RTE.map((trace) => RA.difference(Eq.eqStrict)(ids, trace))
	);

export const secureRead = <Ext>(
	documentKey: unknown,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	refs: NRA.ReadonlyNonEmptyArray<Reference>
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, any> =>
	pipe(
		secureReadQ(documentKey, columns, refs),
		stringifyQuery,
		executeQueryOne,
		RTE.map((result) => result["jsonb_build_object"])
	);

interface SelectQuery {
	ctes: readonly (readonly [string, SelectQuery | string])[];
	select: string;
	from: string;
	where: string;
}

// TODO: express that column is present in value records
const selectColumn = (values: Values, column: Column) =>
	pipe(columnName(column), (primaryKey) => NRA.chain((row: Row) => [row[primaryKey]])(values));

const stringifyQuery = ({ select, from, where, ctes }: SelectQuery): string =>
	pipe(
		ctes,
		RA.map(RT.mapSnd((query) => (typeof query === "string" ? query : stringifyQuery(query)))),
		RA.map(([alias, query]) => `${alias} as (${query})`),
		RA.match(
			() => "",
			(stringifiedCtes) => "with " + joinToString(", ")(stringifiedCtes)
		),
		(cteString) =>
			joinToString("\n")([cteString, select, from, where !== "" ? `where ${where}` : ""])
	);

const executeQueryAny =
	<Ext, R = any>(
		query: string
	): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, readonly R[]> =>
	(context) =>
		TE.tryCatch(() => context.any<R>(query), parseError);

const executeQueryOne =
	<Ext, R = any>(query: string): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, R> =>
	(context) =>
		TE.tryCatch(() => context.one<R>(query), parseError);

const parseError = (err: unknown): Error => {
	if (err instanceof Error) {
		return err;
	} else if (typeof err === "string") {
		return Error(err);
	} else return Error("Unknown error");
};

const insertQ = (
	tableName: string,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	sourceAlias: string
): string => {
	const columnNames = NRA.map(columnName)(columns);
	const sourceQuery = pipe(select(...columnNames), from(sourceAlias), stringifyQuery);

	const destColumns = valueAlias(tableName, columnNames);
	return `INSERT INTO ${destColumns} ${sourceQuery}`;
};

const deleteQ = (tableName: string, primaryKey: Column, sourceAlias: string): string => {
	const escapedTableName = pg.as.alias(tableName);
	const escapedColumnName = pg.as.alias(columnName(primaryKey));

	return `delete from ${escapedTableName}
	where ${escapedColumnName} in (select ${escapedColumnName} from ${sourceAlias})
	returning ${escapedColumnName}`;
};

const updateQ = (
	tableName: string,
	[primary, ...columns]: NRA.ReadonlyNonEmptyArray<Column>,
	currentAlias: string,
	newAlias: string
): string =>
	pipe(
		columns,
		RA.map((column) => [qualifiedColumnName(column), columnName(column)] as const),
		RA.map(
			([qName, name]) =>
				[pg.as.alias(qName), pg.as.alias(`${newAlias}.${name}`), pg.as.alias(name)] as const
		),
		RA.map(([qname, newName, name]) => `${name} = coalesce(${newName}, ${qname})`),
		joinToString(", "),
		(assignments) =>
			pipe(
				[qualifiedColumnName(primary), columnName(primary)] as const,
				([qualified, name]) =>
					[pg.as.alias(qualified), pg.as.alias(`${newAlias}.${name}`)] as const,
				([escapedQualifiedName, escapedName]) => `
					update ${tableName} set ${assignments}
					from ${newAlias}
					where ${escapedQualifiedName} = ${escapedName} and
					${qualifiedColumnName(primary)} in (select ${columnName(primary)} from ${currentAlias})
					returning ${qualifiedColumnName(primary)}
				`
			)
	);

const secureReadQ = (
	documentKey: unknown,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	refs: NRA.ReadonlyNonEmptyArray<Reference>
): SelectQuery =>
	pipe(
		columns,
		NRA.groupBy(qualifiedTableName),
		RR.partitionWithIndex((tableName, _) => tableName === pipe(NRA.head(refs), toTable)),
		SEP.bimap(
			RR.map((cols) =>
				pipe(
					columnKeyValues(cols),
					jsonBuildObject,
					jsonAgg(RA.findFirst((col: Column) => col.isPrimary)(cols))
				)
			),
			RR.map(flow(columnKeyValues, jsonObjectAgg))
		),
		({ left, right }) => RR.union(SG.first<string>())(left)(right),
		jsonBuildObject,
		select,
		from(
			pipe(NRA.head(refs), toTable),
			RA.map(({ from, to }: Reference) => leftJoin(to, from))(refs)
		),
		where(
			eq(
				pipe(NRA.head(refs), fromColumn, qualifiedColumnName, pg.as.alias),
				value(documentKey)
			)
		)
	);

const backtraceExistingQ = (
	documentKey: unknown,
	ids: readonly unknown[],
	primaryKey: Column,
	paths: NRA.ReadonlyNonEmptyArray<Path>
) =>
	pipe(
		select(primaryKey),
		from(qualifiedTableName(primaryKey)),
		where(
			inValueList(primaryKey, ids),
			pipe(
				paths,
				NRA.map((path) => path.length),
				NRA.zip(paths),
				NRA.reduce(
					[Number.POSITIVE_INFINITY, NRA.head(paths)] as const,
					([minLength, minPath], [length, path]) =>
						length < minLength
							? ([length, path] as const)
							: ([minLength, minPath] as const)
				),
				([_, path]) => pipe(backtrace(documentKey, path), stringifyQuery, exists)
			)
		)
	);

const backtraceUpdatesQ = (
	documentKey: unknown,
	values: Values,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	paths: NRA.ReadonlyNonEmptyArray<Path>
): SelectQuery =>
	pipe(
		select(),
		from(valueListWithAlias(values, "t", NRA.map(columnName)(columns))),
		whereA(
			pipe(
				paths,
				NRA.map((path: Path) =>
					pipe(
						backtrace(documentKey, path),
						stringifyQuery,
						exists,
						or(pipe(NRA.head(path), fromColumn, isNull))
					)
				)
			)
		)
	);

const backtraceNewQ = (
	documentKey: unknown,
	values: Values,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	paths: NRA.ReadonlyNonEmptyArray<Path>
): SelectQuery => {
	const augmentedValues = pipe(
		values,
		NRA.mapWithIndex((i, row) => RR.upsertAt("rnum", i as unknown)(row))
	);

	return pipe(
		select(),
		from(valueListWithAlias(augmentedValues, "t", ["rnum", ...NRA.map(columnName)(columns)])),
		whereA(
			pipe(
				paths,
				NRA.map((path: Path) => pipe(backtrace(documentKey, path), stringifyQuery, exists))
			)
		)
	);
};

const backtrace = (documentKey: unknown, path: Path): SelectQuery =>
	RA.size(path) > 1
		? pipe(
				select(),
				from(
					pipe(NRA.head(path), toTable),
					RA.map(({ from, to }: Reference) => innerJoin(from, to))(NRA.tail(path))
				),
				where(
					eq(
						pipe(NRA.head(path), toColumn, qualifiedColumnName, pg.as.alias),
						pg.as.alias(pipe(NRA.head(path), fromColumn, columnName))
					),
					eq(
						pipe(NRA.last(path), toColumn, qualifiedColumnName, pg.as.alias),
						value(documentKey)
					)
				)
		  )
		: pipe(
				select(),
				from(pipe(NRA.head(path), toTable)),
				where(
					eq(
						pipe(NRA.head(path), toColumn, qualifiedColumnName, pg.as.alias),
						value(documentKey)
					)
				)
		  );

// pg.helpers.values is not used here because:
// 1. It depends on an initialized pg-promise instance.
// 2. It does not support conditional casts.
const valueListWithAlias = (
	values: Values,
	tableName: string,
	columnNames: readonly string[]
): string => {
	const alias = valueAlias(tableName, RA.sort(S.Ord)(columnNames));
	const list = valueList(values, columnNames);

	return `(${list}) as ${alias}`;
};
const valueAlias = (tableName: string, columnNames: readonly string[]): string => {
	const escapedTableName = pg.as.alias(tableName);
	const escapedColumnNames = pipe(
		columnNames,
		RA.map((name) => pg.as.alias(name)),
		joinToString(", ")
	);

	return `${escapedTableName} (${escapedColumnNames})`;
};

const valueList = (values: Values, columnNames: readonly string[]): string =>
	pipe(
		format,
		apply(valueListQuery(values.length, columnNames.length)),
		apply(RA.chain(RR.collect(S.Ord)((_, v) => v))(values))
	);

const valueListQuery = (rows: number, columns: number): string =>
	"VALUES " +
	pipe(
		NRA.range(1, rows * columns),
		NRA.map((n) => `$${n}`),
		NRA.chunksOf(columns),
		NRA.map((row) => `(${joinToString(", ")(row)})`),
		joinToString(",\n")
	);

const imputeValues = (
	values: Values,
	columns: readonly Column[],
	def: "default" | null
): Values => {
	const defaultValues = RR.fromFoldableMap(SG.first<unknown>(), RA.Foldable)(
		RA.map((col: Column) => [columnName(col), columnDbType(col)] as const)(columns),
		([name, dbType]) => [name, def === null ? typedNullValue(dbType) : def]
	);

	return pipe(values, NRA.map(RR.union(SG.first<unknown>())(defaultValues)));
};

const typedNullValue = (dbType: string) => () => ({
	toPostgres: () => `null::${dbType}`,
	rawType: true,
});

const flattenToString = (fields: RR.ReadonlyRecord<string, string>): string =>
	pipe(fields, RR.toReadonlyArray, RA.map(RT.mapFst(value)), RA.flatten, joinToString(", "));

const jsonBuildObject = flow(flattenToString, (expr) => `jsonb_build_object(${expr})`);

const jsonObjectAgg = flow(flattenToString, (expr) => `jsonb_object_agg(${expr})`);

const columnKeyValues = (columns: readonly Column[]): RR.ReadonlyRecord<string, string> =>
	pipe(
		columns,
		RA.map((col) => [columnName(col), qualifiedColumnName(col)] as const),
		RR.fromFoldable(SG.first<string>(), RA.Foldable)
	);

const jsonAgg =
	(testColumn: O.Option<Column>) =>
	(expr: string): string =>
		pipe(
			testColumn,
			O.map(flow(qualifiedColumnName, pg.as.alias)),
			O.fold(
				() => `coalesce(jsonb_agg(distinct ${expr}), '[]')`,
				(c) => `coalesce(jsonb_agg(distinct ${expr}) filter (where ${c} is not null), '[]')`
			)
		);

const select = (
	first: string | Column = "*",
	...expr: readonly (string | Column)[]
): SelectQuery => ({
	select: pipe(
		[first, ...expr],
		NRA.map((expr) => (typeof expr === "string" ? expr : pg.as.alias(columnName(expr)))),
		(exprs) => "select " + joinToString(",\n")(exprs)
	),
	from: "",
	where: "",
	ctes: [],
});

const cte =
	(alias: string, cteQuery: SelectQuery | string) =>
	(query: SelectQuery): SelectQuery => ({
		...query,
		ctes: RA.append([alias, cteQuery] as const)(query.ctes),
	});

const innerJoin = (from: Column, to: Column): string =>
	pipe(
		[qualifiedTableName(to), qualifiedColumnName(to), qualifiedColumnName(from)],
		format("inner join $1:alias on ($2:alias = $3:alias)")
	);

const leftJoin = (from: Column, to: Column): string =>
	pipe(
		[qualifiedTableName(to), qualifiedColumnName(to), qualifiedColumnName(from)],
		format("left join $1:alias on ($2:alias = $3:alias)")
	);

const from =
	(tableName: string, join: readonly string[] = []) =>
	(query: SelectQuery): SelectQuery => ({
		...query,
		from: `from ${tableName}\n` + joinToString("\n")(join),
	});

const where =
	(...conditions: NRA.ReadonlyNonEmptyArray<string>) =>
	(query: SelectQuery) =>
		whereA(conditions)(query);

const whereA =
	(conditions: NRA.ReadonlyNonEmptyArray<string>) =>
	(query: SelectQuery): SelectQuery => ({
		...query,
		where: pipe(
			conditions,
			NRA.map((condition) => `(${condition})`),
			(terms) =>
				(query.where !== "" ? query.where + " and " : "") + joinToString(" and ")(terms)
		),
	});

const eq = (a: string, b: string): string => `${a} = ${b}`;

const or =
	(x: string) =>
	(y: string): string =>
		`${x} or ${y}`;

const as = (column: Column, alias: string) => pg.as.alias(columnName(column)) + ` as ${alias}`;

const isNull = (column: Column): string => pg.as.alias(columnName(column)) + " is null";

const exists = (subQuery: string): string => `exists (${subQuery})`;

const value = (v: unknown): string => pg.as.format("$1", v);

const joinToString = (sep: string) => (values: readonly string[]) =>
	intercalate(S.Monoid, RA.Foldable)(sep, values);

const format = (query: string) => (values: readonly unknown[]) => pg.as.format(query, values);

const inValueList = (column: Column, values: readonly unknown[]): string =>
	pg.as.format("$1:alias in ($2:csv)", [columnName(column), values]);
