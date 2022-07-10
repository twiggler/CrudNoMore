import { flow } from "fp-ts/lib/function";

export interface Column<
	Name extends string = any,
	QualifiedTableName extends string = any,
	JsType = any,
	DbType extends string = any,
	IsPrimary extends boolean = any
> {
	readonly name: Name;
	readonly qualifiedTableName: QualifiedTableName;
	readonly dbType: DbType;
	readonly isPrimary: IsPrimary;
}

export interface Reference {
	readonly from: Column;
	readonly to: Column;
}

export const qualifiedTableName = (column: Column): string => column.qualifiedTableName;

export const qualifiedColumnName = (column: Column): string =>
	`${qualifiedTableName(column)}.${column.name}`;

export const columnName = (column: Column): string => column.name;

export const columnDbType = (column: Column): string => column.dbType;

export const isPrimary = (column: Column): boolean => column.isPrimary;

export const fromColumn = (ref: Reference) => ref.from;

export const toColumn = (ref: Reference) => ref.to;

export const toTable = flow(toColumn, qualifiedTableName);

export const fromTable = flow(fromColumn, qualifiedTableName);
