import { apply, flow, pipe } from "fp-ts/lib/function";
import * as RA from "fp-ts/lib/ReadonlyArray";
import * as NRA from "fp-ts/lib/ReadonlyNonEmptyArray";
import * as RR from "fp-ts/lib/ReadonlyRecord";
import * as S from "fp-ts/String";
import { intercalate } from "fp-ts/lib/Foldable";
import {
	Column,
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
import * as pg from "pg-promise";
import * as RT from "fp-ts/lib/ReadonlyTuple";
import * as TE from "fp-ts/lib/TaskEither";
import * as N from "fp-ts/lib/number";
import * as SEP from "fp-ts/lib/Separated";
import * as RTE from "fp-ts/lib/ReaderTaskEither";

type Rows = RR.ReadonlyRecord<string, unknown>;

type Values = NRA.ReadonlyNonEmptyArray<Rows>;

export type Path = NRA.ReadonlyNonEmptyArray<Reference>;

export const secureInsert =
	<Ext>(
		documentKey: unknown,
		values: Values,
		columns: NRA.ReadonlyNonEmptyArray<Column>,
		paths: NRA.ReadonlyNonEmptyArray<Path>
	): RTE.ReaderTaskEither<pg.IDatabase<Ext>, Error, readonly number[]> =>
	(db) =>
		dbTask(
			db,
			`Insert: table ${pipe(columns, NRA.head, qualifiedTableName)}`,
			pipe(
				select("rnum"),
				from("trace"),
				cte("trace", backtraceForeignKeysQ(documentKey, values, columns, paths)),
				cte("ir", insertQ(columns, "trace")),
				stringifyQuery,
				executeQueryAny,
				RTE.map(RA.map((result: { rnum: number }) => result.rnum)),
				RTE.map((trace) => RA.difference(N.Eq)(NRA.range(0, values.length - 1), trace))
			)
		);

export const secureRead =
	<Ext>(
		documentKey: unknown,
		columns: NRA.ReadonlyNonEmptyArray<Column>,
		refs: NRA.ReadonlyNonEmptyArray<Reference>
	): RTE.ReaderTaskEither<pg.IDatabase<Ext>, Error, any> =>
	(db) =>
		dbTask(
			db,
			`Read document`,
			pipe(
				secureReadQ(documentKey, columns, refs),
				stringifyQuery,
				executeQueryOne,
				RTE.map((result) => result["jsonb_build_object"])
			)
		);

const dbTask =
	<Ext, M>(
		db: pg.IDatabase<Ext>,
		tag: string,
		cb: (t: pg.ITask<Ext>) => TE.TaskEither<any, M>
	): TE.TaskEither<any, M> =>
	() =>
		db.task(tag, (t) => cb(t)());

interface SelectQuery {
	ctes: readonly (readonly [string, SelectQuery | string])[];
	select: string;
	from: string;
	where: string;
}

const stringifyQuery = ({ select, from, where, ctes }: SelectQuery): string =>
	pipe(
		ctes,
		RA.map(RT.mapSnd((query) => (typeof query === "string" ? query : stringifyQuery(query)))),
		RA.map(([alias, query]) => `${alias} as (${query})`),
		RA.match(
			() => "",
			(stringifiedCtes) => "with " + joinToString(", ")(stringifiedCtes)
		),
		(cteString) => joinToString("\n")([cteString, select, from, where])
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

const insertQ = (columns: NRA.ReadonlyNonEmptyArray<Column>, sourceAlias: string): string => {
	const tableName = pipe(NRA.head(columns), qualifiedTableName);
	const columnNames = NRA.map(columnName)(columns);
	const sourceQuery = pipe(select(...columnNames), from(sourceAlias), stringifyQuery);

	const destColumns = valueAlias(tableName, columnNames);
	return `INSERT INTO ${destColumns} ${sourceQuery}`;
};

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

const backtraceForeignKeysQ = (
	documentKey: unknown,
	values: Values,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	paths: NRA.ReadonlyNonEmptyArray<Path>
): SelectQuery => {
	const foreignKeys = pipe(
		paths,
		NRA.map((path: Path) => pipe(NRA.head(path), fromColumn, columnName))
	);
	const augmentedValues = pipe(
		values,
		NRA.mapWithIndex((i, row) => RR.upsertAt("rnum", i as unknown)(row))
	);

	return pipe(
		selectAll(),
		from(valueListWithAlias(augmentedValues, "t", ["rnum", ...NRA.map(columnName)(columns)])),
		whereA(
			pipe(
				NRA.zip(foreignKeys, paths),
				NRA.map(([name, path]) =>
					pipe(backtrace(documentKey, name, path), stringifyQuery, exists)
				)
			)
		)
	);
};

const backtrace = (documentKey: unknown, startColumnAlias: string, path: Path): SelectQuery =>
	RA.size(path) > 1
		? pipe(
				selectAll(),
				from(
					pipe(NRA.head(path), toTable),
					RA.map(({ from, to }: Reference) => innerJoin(from, to))(NRA.tail(path))
				),
				where(
					eq(
						pipe(NRA.head(path), toColumn, qualifiedColumnName, pg.as.alias),
						pg.as.alias(startColumnAlias)
					),
					eq(
						pipe(NRA.last(path), toColumn, qualifiedColumnName, pg.as.alias),
						value(documentKey)
					)
				)
		  )
		: pipe(
				selectAll(),
				from(pipe(NRA.head(path), toTable)),
				where(
					eq(
						pipe(NRA.head(path), toColumn, qualifiedColumnName, pg.as.alias),
						value(documentKey)
					)
				)
		  );

const valueList = (values: Values, columnNames: readonly string[]): string =>
	pipe(
		format,
		apply(valueListQuery(values.length, columnNames.length)),
		apply(imputeValues(values, columnNames))
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

const imputeValues = (values: Values, columnNames: readonly string[]): readonly unknown[] => {
	const defaultValues = RR.fromFoldableMap(SG.first<string>(), RA.Foldable)(
		columnNames,
		(key) => [key, "DEFAULT"]
	);

	return pipe(
		values,
		NRA.map(RR.union(SG.first<unknown>())(defaultValues)),
		RA.chain(RR.collect(S.Ord)((_, v) => v))
	);
};

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

const valueListWithAlias = (
	values: Values,
	tableName: string,
	columnNames: readonly string[]
): string =>
	`(${valueList(values, columnNames)}) as ${valueAlias(tableName, RA.sort(S.Ord)(columnNames))}`;

const valueAlias = (tableName: String, columnNames: readonly string[]): string =>
	`${tableName} (${joinToString(", ")(columnNames)})`;

const select = (...expr: readonly string[]): SelectQuery => ({
	select: "select " + joinToString(",\n")(expr),
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

const selectAll = (): SelectQuery => select("*");

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
		where: "where " + joinToString(" and ")(conditions),
	});

const eq = (a: string, b: string): string => `${a} = ${b}`;

const exists = (subQuery: string): string => `exists (${subQuery})`;

const value = (v: unknown): string => pg.as.format("$1", v);

const joinToString = (sep: string) => (values: readonly string[]) =>
	intercalate(S.Monoid, RA.Foldable)(sep, values);

const format = (query: string) => (values?: any) => pg.as.format(query, values);
