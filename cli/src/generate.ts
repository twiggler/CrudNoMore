import pgPromise from "pg-promise";
import { QueryFile, utils, IInitOptions } from "pg-promise";
import * as t from "io-ts";
import { failure } from "io-ts/PathReporter";
import { nonEmptyArray as ioNonEmptyArray, NonEmptyString } from "io-ts-types";
import { pipe } from "fp-ts/lib/function";
import { taskEither, nonEmptyArray, task } from "fp-ts";
import { NonEmptyArray } from "fp-ts/lib/NonEmptyArray";
import * as E from "fp-ts/lib/Either";
import * as A from "fp-ts/lib/Array";
import * as O from "fp-ts/lib/Option";
import * as C from "fp-ts/lib/Console";
import * as SG from "fp-ts/lib/Semigroup";
import * as M from "fp-ts/lib/Map";
import * as STR from "fp-ts/lib/string";
import * as ts from "typescript";
import * as fs from "fs";
import { promisify } from "util";
import { sequenceT } from "fp-ts/lib/Apply";
import yargs from "yargs";

type ColumnIdentifierById = Map<Column["id"], ts.Identifier>;

const schemaQueryResultCodec = ioNonEmptyArray(
	t.type({
		id: t.string,
		columnReference: t.union([t.null, t.string]),
		tableName: NonEmptyString,
		columnName: NonEmptyString,
		columnDataType: NonEmptyString,
		columnOrder: t.number,
		primaryKey: t.boolean,
	})
);

type SchemaQueryResult = t.TypeOf<typeof schemaQueryResultCodec>;
type Column = SchemaQueryResult[number];

const lookupColumnIdentifier = (
	key: Column["id"],
	map: ColumnIdentifierById
): E.Either<string[], ts.Identifier> =>
	pipe(
		M.lookup(STR.Eq)(key, map),
		E.fromOption(() => [`Logic error: cannot find identifier for key ${key}`])
	);

const lookupColumnIdentifiers = (from: Column["id"], to: Column["id"], map: ColumnIdentifierById) =>
	sequenceT(E.Applicative)(lookupColumnIdentifier(from, map), lookupColumnIdentifier(to, map));

const initOptions: IInitOptions = {
	receive(data: any[]) {
		camelizeColumns(data);
	},
};

const camelizeColumns = (data: any[]) => {
	if (data.length === 0) return;
	const tmp: Record<string, any> = data[0];
	for (const prop in tmp) {
		const camel = utils.camelize(prop);
		if (!(camel in tmp)) {
			for (let i = 0; i < data.length; i++) {
				const d = data[i];
				d[camel] = d[prop];
				delete d[prop];
			}
		}
	}
};

const querySchema = async (connectionString: string): Promise<any[]> => {
	const pgp = pgPromise(initOptions);
	const db = pgp(connectionString);

	const schemaQuery = new QueryFile("./schema.sql");
	const schemaQueryResult = await db.many(schemaQuery);
	await db.$pool.end();
	return schemaQueryResult;
};

const jsType = (dataType: string): string => {
	switch (dataType) {
		case "text":
			return "string";
		case "bigint":
			return "number";
		default:
			return "never";
	}
};

const makeColumnNodes = (column: Column): [ColumnIdentifierById, NonEmptyArray<ts.Node>] => {
	const qualifedColumnName = utils.camelizeVar(`${column.tableName}_${column.columnName}`);
	const isPrimaryNode = column.primaryKey ? ts.factory.createTrue() : ts.factory.createFalse();
	const columnTypeReference = ts.factory.createTypeReferenceNode("Column", [
		ts.factory.createLiteralTypeNode(ts.factory.createStringLiteral(column.columnName)),
		ts.factory.createLiteralTypeNode(ts.factory.createStringLiteral(column.tableName)),
		ts.factory.createTypeReferenceNode(jsType(column.columnDataType)),
		ts.factory.createLiteralTypeNode(ts.factory.createStringLiteral(column.columnDataType)),
		ts.factory.createLiteralTypeNode(isPrimaryNode),
	]);

	const columnIdentifier = ts.factory.createUniqueName(
		qualifedColumnName,
		ts.GeneratedIdentifierFlags.Optimistic
	);

	const columnType = ts.factory.createTypeAliasDeclaration(
		undefined,
		ts.factory.createModifiersFromModifierFlags(ts.ModifierFlags.Export),
		columnIdentifier,
		[],
		columnTypeReference
	);

	const columnVar = ts.factory.createVariableStatement(
		ts.factory.createModifiersFromModifierFlags(ts.ModifierFlags.Export),
		ts.factory.createVariableDeclarationList(
			[
				ts.factory.createVariableDeclaration(
					qualifedColumnName,
					undefined,
					ts.factory.createTypeReferenceNode(columnIdentifier),
					ts.factory.createObjectLiteralExpression([
						ts.factory.createPropertyAssignment(
							"name",
							ts.factory.createStringLiteral(column.columnName)
						),
						ts.factory.createPropertyAssignment(
							"qualifiedTableName",
							ts.factory.createStringLiteral(column.tableName)
						),
						ts.factory.createPropertyAssignment("isPrimary", isPrimaryNode),
					])
				),
			],
			ts.NodeFlags.Const
		)
	);

	return [M.singleton(column.id, columnIdentifier), [columnType, columnVar]];
};

const makeReferenceNode = (from: ts.Identifier, to: ts.Identifier): ts.Node => {
	const referenceIdentifier = ts.factory.createUniqueName(
		`${to.text}By${from.text}`,
		ts.GeneratedIdentifierFlags.Optimistic
	);
	return ts.factory.createVariableStatement(
		ts.factory.createModifiersFromModifierFlags(ts.ModifierFlags.Export),
		ts.factory.createVariableDeclarationList(
			[
				ts.factory.createVariableDeclaration(
					referenceIdentifier,
					undefined,
					ts.factory.createTypeReferenceNode("Reference"),
					ts.factory.createObjectLiteralExpression([
						ts.factory.createPropertyAssignment("from", from),
						ts.factory.createPropertyAssignment("to", to),
					])
				),
			],
			ts.NodeFlags.Const
		)
	);
};

const makePreambleNode = (): ts.Node => {
	const importNode = ts.factory.createImportDeclaration(
		/* decorators */ undefined,
		/* modifiers */ undefined,
		ts.factory.createImportClause(
			false,
			undefined,
			ts.factory.createNamedImports([
				ts.factory.createImportSpecifier(
					false,
					undefined,
					ts.factory.createIdentifier("Column")
				),
				ts.factory.createImportSpecifier(
					false,
					undefined,
					ts.factory.createIdentifier("Reference")
				),
			])
		),
		ts.factory.createStringLiteral("./schema")
	);
	ts.addSyntheticLeadingComment(
		importNode,
		ts.SyntaxKind.MultiLineCommentTrivia,
		"DO NOT EDIT - THIS IS A GENERATED FILE",
		true
	);

	return importNode;
};

const generateAST = (columns: SchemaQueryResult): E.Either<string[], ts.Node[]> =>
	pipe(
		columns,
		nonEmptyArray.map(makeColumnNodes),
		nonEmptyArray.concatAll(
			SG.tuple(
				M.getUnionSemigroup(STR.Eq, SG.last<ts.Identifier>()),
				A.getSemigroup<ts.Node>()
			)
		),
		([identifierMap, tableNodes]) =>
			pipe(
				columns,
				A.filterMap(({ id, columnReference }) =>
					sequenceT(O.Applicative)(O.some(id), O.fromNullable(columnReference))
				),
				A.map((ids) => lookupColumnIdentifiers(...ids, identifierMap)),
				A.traverse(E.Applicative)(
					E.map((identifiers) => makeReferenceNode(...identifiers))
				),
				E.map((referenceNodes) => [makePreambleNode(), ...tableNodes, ...referenceNodes])
			)
	);

const unsafeWriteCode = (outPath: string, nodes: ts.Node[]): string => {
	const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });

	const sourceFile = ts.createSourceFile(outPath, "", ts.ScriptTarget.Latest, true);

	const nodeArray = ts.factory.createNodeArray(nodes);
	return printer.printList(ts.ListFormat.MultiLine, nodeArray, sourceFile);
};

const writeCode = (outPath: string) => (nodes: ts.Node[]) =>
	E.tryCatch(() => unsafeWriteCode(outPath, nodes), resolveError);

const isValidationError = (err: unknown): err is t.ValidationError[] =>
	Array.isArray(err) && err.every((e) => "value" in e && "context" in e);

const resolveError = (err: any): string[] => {
	if (err instanceof Error) {
		return [err.message];
	} else if (typeof err === "string") {
		return [err];
	} else if (isValidationError(err)) {
		return failure(err);
	} else return ["Unknown error"];
};

const writeFile = taskEither.tryCatchK(promisify(fs.writeFile), resolveError);

const main = (connectionString: string, outPath: string) =>
	pipe(
		connectionString,
		taskEither.tryCatchK(querySchema, resolveError),
		taskEither.chainEitherKW(schemaQueryResultCodec.decode),
		taskEither.chainEitherKW(generateAST),
		taskEither.chainEitherKW(writeCode(outPath)),
		taskEither.chainW((content) => writeFile(outPath, content)),
		taskEither.fold(
			(errors) => pipe(errors, resolveError, A.getShow(STR.Show).show, C.error, task.fromIO),
			(_) => async () => {}
		)
	);

yargs
	.scriptName("twigSync-cli")
	.usage("$0 <cmd> [args]")
	.command(
		"generate",
		"Generate typescript code from the database schema",
		(yargs) =>
			yargs
				.positional("connection", {
					type: "string",
					describe: "Connection string of the database to connect to",
				})
				.positional("outfile", {
					type: "string",
					describe: "Path of the output file",
				})
				.demandOption(["connection", "outfile"])
				.normalize("outfile"),
		(argv): Promise<void> => main(argv.connection, argv.outfile)()
	)
	.demandCommand(1, "You need at least one command before moving on")
	.help().argv;
