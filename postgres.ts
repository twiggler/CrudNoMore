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
import * as pg from "pg-promise";
import * as TE from "fp-ts/lib/TaskEither";
import * as N from "fp-ts/lib/number";
import * as SEP from "fp-ts/lib/Separated";
import * as RTE from "fp-ts/lib/ReaderTaskEither";

export type Rows = RR.ReadonlyRecord<string, unknown>;

type Values = NRA.ReadonlyNonEmptyArray<Rows>;

export type Path = NRA.ReadonlyNonEmptyArray<Reference>;

export type TraceResult = SEP.Separated<readonly Rows[], readonly Rows[]>;

export const secureInsert =
	<Ext>(
		documentKey: unknown,
		values: Values,
		columns: NRA.ReadonlyNonEmptyArray<Column>,
		paths: NRA.ReadonlyNonEmptyArray<Path>
	): RTE.ReaderTaskEither<pg.IDatabase<Ext>, Error, TraceResult> =>
	(db) =>
		dbTask(
			db,
			`Insert: table ${pipe(columns, NRA.head, qualifiedTableName)}`,
			pipe(
				backtraceForeignKeys(documentKey, values, paths),
				RTE.chain((traceResult) =>
					pipe(
						RA.isNonEmpty(traceResult.right)
							? insert(traceResult.right, columns)
							: RTE.of([]),
						RTE.map((_) => traceResult)
					)
				)
			)
		);

export const secureRead =
	<Ext, R>(
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
				executeQueryOne,
				RTE.map((result) => result["json_build_object"])
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

const backtraceForeignKeys = <Ext>(
	documentKey: unknown,
	values: Values,
	paths: NRA.ReadonlyNonEmptyArray<Path>
): RTE.ReaderTaskEither<pg.IBaseProtocol<Ext>, Error, TraceResult> =>
	pipe(
		backtraceForeignKeysQ(documentKey, values, paths),
		executeQueryAny,
		RTE.map(RA.map((result: { rnum: number }) => result.rnum)),
		RTE.map((tracedRowNumbers) =>
			RA.partitionWithIndex((i, _) => RA.elem(N.Eq)(i, tracedRowNumbers))(values)
		)
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

const insertQ = (values: Values, columns: NRA.ReadonlyNonEmptyArray<Column>): string => {
	const tableName = pipe(NRA.head(columns), qualifiedTableName);
	const columnNames = NRA.map(columnName)(columns);

	return `INSERT INTO ${valueAlias(tableName, columnNames)} ${valueList(values, columnNames)}`;
};

const secureReadQ = (
	documentKey: unknown,
	columns: NRA.ReadonlyNonEmptyArray<Column>,
	refs: NRA.ReadonlyNonEmptyArray<Reference>
): string =>
	query(
		select(
			jsonBuildObject(
				pipe(
					columns,
					NRA.groupBy(qualifiedTableName),
					RR.partitionWithIndex(
						(tableName, _) => tableName === pipe(NRA.head(refs), toTable)
					),
					SEP.bimap(
						RR.collect(S.Ord)(
							(tableName, columns) =>
								[
									`'${tableName}'`,
									pipe(columns, columnKeyValues, jsonBuildObject, jsonAgg),
								] as const
						),
						RR.collect(S.Ord)(
							(tableName, columns) =>
								[
									`'${tableName}'`,
									pipe(columns, columnKeyValues, jsonObjectAgg),
								] as const
						)
					),
					({ left, right }) => RA.concat(left)(right)
				)
			)
		),
		from(pipe(NRA.head(refs), toTable)),
		...RA.map(({ from, to }: Reference) => leftJoin(to, from))(refs),
		where(
			eq(
				pipe(NRA.head(refs), fromColumn, qualifiedColumnName, pg.as.alias),
				value(documentKey)
			)
		)
	);

const insert = flow(insertQ, executeQueryAny);

const backtraceForeignKeysQ = (
	documentKey: unknown,
	values: Values,
	paths: NRA.ReadonlyNonEmptyArray<Path>
): string => {
	const foreignKeys = pipe(
		paths,
		NRA.map((path: Path) => pipe(NRA.head(path), fromColumn, columnName))
	);
	const augmentedValues = pipe(
		values,
		NRA.map(RR.filterWithIndex((columnName) => RA.elem(S.Eq)(columnName, foreignKeys))),
		NRA.mapWithIndex((i, row) => RR.upsertAt("rnum", i as unknown)(row))
	);

	return query(
		select("rnum"),
		from(valueListWithAlias(augmentedValues, "t", ["rnum", ...foreignKeys])),
		whereA(
			pipe(
				NRA.zip(foreignKeys, paths),
				NRA.map(([name, path]) => exists(backtrace(documentKey, name, path)))
			)
		)
	);
};

const backtrace = (documentKey: unknown, startColumnAlias: string, path: Path): string =>
	RA.size(path) > 1
		? query(
				selectAll(),
				from(pipe(NRA.head(path), toTable)),
				...RA.map(({ from, to }: Reference) => innerJoin(from, to))(NRA.tail(path)),
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
		: query(
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

const jsonBuildObject = (fields: readonly (readonly [string, string])[]): string =>
	pipe(RA.flatten(fields), joinToString(", "), (args) => `json_build_object(${args})`);

const jsonObjectAgg = (fields: readonly (readonly [string, string])[]): string =>
	pipe(RA.flatten(fields), joinToString(", "), (args) => `json_object_agg(${args})`);

const columnKeyValues = (columns: readonly Column[]): readonly (readonly [string, string])[] =>
	pipe(
		columns,
		RA.map((col) => [`'${columnName(col)}'`, qualifiedColumnName(col)] as const)
	);

const jsonAgg = (expr: string): string => `coalesce(json_agg((${expr})), '[]')`;

const valueListWithAlias = (
	values: Values,
	tableName: string,
	columnNames: readonly string[]
): string =>
	`(${valueList(values, columnNames)}) as ${valueAlias(tableName, RA.sort(S.Ord)(columnNames))}`;

const valueAlias = (tableName: String, columnNames: readonly string[]): string =>
	`${tableName} (${joinToString(", ")(columnNames)})`;

const query = (...expr: readonly string[]): string => joinToString("\n")(expr);

const select = (...expr: readonly string[]): string => "select " + joinToString(",\n")(expr);

const selectAll = (): string => "select *";

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

const from = (expr: string): string => `from ${expr}`;

const where = (...conditions: NRA.ReadonlyNonEmptyArray<string>): string => whereA(conditions);

const whereA = (conditions: NRA.ReadonlyNonEmptyArray<string>): string =>
	"where " + joinToString(" and ")(conditions);

const eq = (a: string, b: string): string => `${a} = ${b}`;

const exists = (subQuery: string): string => `exists (${subQuery})`;

const value = (v: unknown): string => pg.as.format("$1", v);

const joinToString = (sep: string) => (values: readonly string[]) =>
	intercalate(S.Monoid, RA.Foldable)(sep, values);

const format = (query: string) => (values?: any) => pg.as.format(query, values);
