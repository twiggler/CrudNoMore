import * as O from "fp-ts/lib/Option";
import { apply, pipe } from "fp-ts/lib/function";
import * as RA from "fp-ts/lib/ReadonlyArray";
import * as NRA from "fp-ts/lib/ReadonlyNonEmptyArray";
import * as Tr from "fp-ts/lib/Tree";
import * as T from "fp-ts/lib/Tuple";
import * as M from "fp-ts/lib/Map";
import * as S from "fp-ts/lib/string";
import * as RR from "fp-ts/lib/ReadonlyRecord";
import * as SG from "fp-ts/lib/Semigroup";

export type NodeType = string;

export interface Graph<L> {
	nodes: readonly NodeType[];
	edges: readonly Edge<L>[];
}

export interface Edge<L> {
	from: NodeType;
	to: NodeType;
	label: L;
}

export interface Context<L> {
	node: NodeType;
	incomingEdges: readonly Edge<L>[];
	outgoingEdges: readonly Edge<L>[];
}

export type Error = string;

export const makeGraph = <N, L>(
	nodes: readonly NodeType[],
	edges: readonly Edge<L>[]
): Graph<L> => ({
	nodes,
	edges,
});

export const matchNode = <L>(node: NodeType, graph: Graph<L>): [O.Option<Context<L>>, Graph<L>] =>
	pipe(
		RA.findIndex((n: NodeType) => node === n)(graph.nodes),
		O.bindTo("index"),
		O.bind("edges", () => O.some(classifyEdges(node, graph.edges))),
		O.bind("context", ({ edges }) =>
			O.some(makeContext(node, edges["Incoming"], edges["Outgoing"]))
		),
		O.bind("rest", ({ index, edges }) =>
			O.some(makeGraph(RA.unsafeDeleteAt(index, graph.nodes), edges["Rest"]))
		),
		O.fold(
			() => [O.none, graph],
			({ context, rest }) => [O.some(context), rest]
		)
	);

export const matchNodeS = <L>(node: NodeType, graph: Graph<L>): O.Option<[Context<L>, Graph<L>]> =>
	pipe(matchNode(node, graph), T.sequence(O.Applicative));

export const precedingEdgesE = <L>(
	edges: readonly Edge<L>[],
	graph: Graph<L>
): Map<NodeType, Edge<L>> => {
	const go = (
		[edge, ...edges]: NRA.ReadonlyNonEmptyArray<Edge<L>>,
		graph: Graph<L>
	): Map<NodeType, Edge<L>> =>
		pipe(
			matchNodeS(edge.to, graph),
			O.fold(
				() => precedingEdgesE(edges, graph),
				([context, graph]) => {
					const suc = precedingEdgesE([...context.outgoingEdges, ...edges], graph);
					return M.upsertAt(S.Eq)(context.node, edge)(suc);
				}
			)
		);

	return RA.isNonEmpty(edges) ? go(edges, graph) : new Map();
};

export const precedingEdges = <L>(node: NodeType, graph: Graph<L>): Map<NodeType, Edge<L>> =>
	pipe(
		matchNodeS(node, graph),
		O.fold(
			() => new Map(),
			([context, graph]) => precedingEdgesE(context.outgoingEdges, graph)
		)
	);

export const topsort = <L>(node: NodeType, igraph: Graph<L>): readonly NodeType[] =>
	pipe(df([node], igraph), T.fst, postorderF, RA.reverse);

const df = <L>(nodes: readonly NodeType[], graph: Graph<L>): [Tr.Forest<NodeType>, Graph<L>] => {
	const go = (
		[node, ...nodes]: NRA.ReadonlyNonEmptyArray<NodeType>,
		graph: Graph<L>
	): [Tr.Forest<NodeType>, Graph<L>] =>
		pipe(
			matchNodeS(node, graph),
			O.bindTo("match"),
			O.bind("suc", ({ match: [context, graph] }) => O.some(df(succ(context), graph))),
			O.bind("sib", ({ suc: [_, graph] }) => O.some(df(nodes, graph))),
			O.fold(
				() => df(nodes, graph),
				({ suc: [t1], sib: [t2, graph], match: [context] }) => [
					[Tr.make(context.node, t1), ...t2],
					graph,
				]
			)
		);

	return RA.isNonEmpty(nodes) ? go(nodes, graph) : [[], graph];
};

export const postorder = <V>(tree: Tr.Tree<V>): readonly V[] => [
	...postorderF(tree.forest),
	tree.value,
];

const postorderF = <V>(forest: Tr.Forest<V>): readonly V[] =>
	RA.foldMap(RA.getMonoid<V>())((tree: Tr.Tree<V>) => postorder(tree))(forest);

const makeContext = <L>(
	node: NodeType,
	incomingEdges: readonly Edge<L>[],
	outgoingEdges: readonly Edge<L>[]
): Context<L> => ({
	node,
	incomingEdges,
	outgoingEdges,
});

const succ = <L>(context: Context<L>) => RA.map((edge: Edge<L>) => edge.to)(context.outgoingEdges);

type EdgeType = "Incoming" | "Outgoing" | "Rest";

const classifyEdge =
	<L>(node: NodeType) =>
	({ from, to }: Edge<L>): EdgeType =>
		node === from ? "Outgoing" : node === to ? "Incoming" : "Rest";

type GroupedEdges<L> = Record<EdgeType, readonly Edge<L>[]>;

const classifyEdges = <L>(node: NodeType, edges: readonly Edge<L>[]): GroupedEdges<L> =>
	pipe(
		edges,
		NRA.groupBy(classifyEdge(node)),
		RR.union(SG.first<readonly Edge<L>[]>())({
			Incoming: [],
			Outgoing: [],
			Rest: [],
		})
	) as GroupedEdges<L>;
